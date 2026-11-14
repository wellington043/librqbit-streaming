#[cfg(windows)]
pub fn mark_file_sparse(f: &std::fs::File) -> bool {
    use std::os::windows::io::AsRawHandle;
    use windows::{
        Win32::Foundation::HANDLE, Win32::System::IO::DeviceIoControl,
        Win32::System::Ioctl::FSCTL_SET_SPARSE,
    };

    let handle = HANDLE(f.as_raw_handle());

    unsafe { DeviceIoControl(handle, FSCTL_SET_SPARSE, None, 0, None, 0, None, None).is_ok() }
}

/// Reclaim an exact byte range. Never expand into a neighbouring live piece.
pub fn discard_range(file: &std::fs::File, offset: u64, length: u64) -> anyhow::Result<()> {
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows::Win32::{
            Foundation::HANDLE,
            Storage::FileSystem::{BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle},
        };
        let mut information = BY_HANDLE_FILE_INFORMATION::default();
        unsafe {
            GetFileInformationByHandle(HANDLE(file.as_raw_handle()), &mut information)?;
        }
        anyhow::ensure!(
            information.nNumberOfLinks == 1,
            "refusing to reclaim a hard-linked payload"
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        anyhow::ensure!(
            file.metadata()?.nlink() == 1,
            "refusing to reclaim a hard-linked payload"
        );
    }
    let end = offset
        .checked_add(length)
        .ok_or_else(|| anyhow::anyhow!("discard range overflow"))?;
    anyhow::ensure!(end <= file.metadata()?.len(), "discard range exceeds file");
    if length == 0 {
        return Ok(());
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows::Win32::{
            Foundation::HANDLE,
            System::{
                IO::DeviceIoControl,
                Ioctl::{FILE_ZERO_DATA_INFORMATION, FSCTL_SET_ZERO_DATA},
            },
        };
        anyhow::ensure!(
            mark_file_sparse(file),
            "filesystem does not support sparse files"
        );
        let input = FILE_ZERO_DATA_INFORMATION {
            FileOffset: offset.try_into()?,
            BeyondFinalZero: end.try_into()?,
        };
        unsafe {
            DeviceIoControl(
                HANDLE(file.as_raw_handle()),
                FSCTL_SET_ZERO_DATA,
                Some((&input as *const FILE_ZERO_DATA_INFORMATION).cast()),
                std::mem::size_of_val(&input) as u32,
                None,
                0,
                None,
                None,
            )?;
        }
        file.sync_all()?;
        Ok(())
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        #[cfg(target_os = "linux")]
        use nix::libc::fallocate;
        // Android's off_t is only 32 bits on ARMv7; use its explicit 64-bit ABI.
        #[cfg(target_os = "android")]
        use nix::libc::fallocate64 as fallocate;
        use std::os::fd::AsRawFd;

        let offset = offset.try_into()?;
        let length = length.try_into()?;
        loop {
            let result = unsafe {
                fallocate(
                    file.as_raw_fd(),
                    nix::libc::FALLOC_FL_PUNCH_HOLE | nix::libc::FALLOC_FL_KEEP_SIZE,
                    offset,
                    length,
                )
            };
            if result == 0 {
                break;
            }
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted {
                return Err(error.into());
            }
        }
        file.sync_all()?;
        Ok(())
    }
    #[cfg(not(any(windows, target_os = "linux", target_os = "android")))]
    anyhow::bail!("physical range reclamation is unsupported on this platform")
}

#[cfg(all(test, any(windows, target_os = "linux", target_os = "android")))]
mod tests {
    use super::*;
    use std::io::{Read, Seek, SeekFrom, Write};
    #[test]
    fn reclaim_hardlinked_payload_preserves_external_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let payload = dir.path().join("cache");
        let external = dir.path().join("retained");
        std::fs::write(&payload, vec![0x5a; 128 * 1024]).unwrap();
        std::fs::hard_link(&payload, &external).unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(payload)
            .unwrap();
        assert!(discard_range(&file, 0, 128 * 1024).is_err());
        assert!(discard_range(&file, 0, 0).is_err());
        assert_eq!(std::fs::read(external).unwrap(), vec![0x5a; 128 * 1024]);
    }

    #[cfg(unix)]
    #[test]
    fn reclaim_unlinked_file_is_rejected_and_preserves_payload() {
        use std::os::unix::fs::MetadataExt;
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&[0x5a; 128]).unwrap();
        assert_eq!(file.metadata().unwrap().nlink(), 0);
        assert!(discard_range(&file, 0, 128).is_err());
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, [0x5a; 128]);
    }

    #[test]
    fn reclaim_sparse_range_preserves_length_and_neighbour_bytes() {
        let (mut file, _path) = tempfile::NamedTempFile::new().unwrap().into_parts();
        file.write_all(&vec![0x5a; 256 * 1024]).unwrap();
        discard_range(&file, 64 * 1024, 128 * 1024).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 256 * 1024);
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert!(bytes[..64 * 1024].iter().all(|b| *b == 0x5a));
        assert!(bytes[64 * 1024..192 * 1024].iter().all(|b| *b == 0));
        assert!(bytes[192 * 1024..].iter().all(|b| *b == 0x5a));
    }

    #[test]
    fn reclaim_unaligned_range_preserves_exact_neighbour_bytes() {
        let (mut file, _path) = tempfile::NamedTempFile::new().unwrap().into_parts();
        file.write_all(&vec![0x5a; 256 * 1024]).unwrap();
        discard_range(&file, 65_553, 131_163).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 262_144);
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert!(bytes[..65_553].iter().all(|b| *b == 0x5a));
        assert!(bytes[65_553..196_716].iter().all(|b| *b == 0));
        assert!(bytes[196_716..].iter().all(|b| *b == 0x5a));
    }

    #[test]
    fn reclaim_invalid_ranges_preserves_payload() {
        let (mut file, _path) = tempfile::NamedTempFile::new().unwrap().into_parts();
        file.write_all(&[0x5a; 128]).unwrap();
        for (offset, length) in [(u64::MAX, 1), (127, 2), (129, 0), (0, u64::MAX)] {
            assert!(discard_range(&file, offset, length).is_err());
        }
        assert_eq!(file.metadata().unwrap().len(), 128);
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, [0x5a; 128]);
    }

    #[test]
    fn reclaim_zero_length_preserves_payload_and_accepts_eof() {
        let (mut file, _path) = tempfile::NamedTempFile::new().unwrap().into_parts();
        file.write_all(&[0x5a; 128]).unwrap();
        for offset in [0, 64, 128] {
            discard_range(&file, offset, 0).unwrap();
        }
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, [0x5a; 128]);
    }

    #[test]
    fn reclaim_readonly_file_returns_error_and_preserves_payload() {
        let dir = tempfile::tempdir().unwrap();
        let payload = dir.path().join("cache");
        std::fs::write(&payload, vec![0x5a; 128 * 1024]).unwrap();
        let file = std::fs::File::open(&payload).unwrap();
        assert!(discard_range(&file, 0, 128 * 1024).is_err());
        assert_eq!(std::fs::read(payload).unwrap(), vec![0x5a; 128 * 1024]);
    }

    #[test]
    fn reclaim_range_above_4gib_preserves_low_bytes_and_neighbours() {
        let (mut file, _path) = tempfile::NamedTempFile::new().unwrap().into_parts();
        #[cfg(windows)]
        assert!(mark_file_sparse(&file));
        let high_start = (1_u64 << 32) + 65_536;
        let file_length = high_start + 262_144;
        file.set_len(file_length).unwrap();
        file.write_all(&vec![0x3c; 262_144]).unwrap();
        file.seek(SeekFrom::Start(high_start)).unwrap();
        file.write_all(&vec![0x5a; 262_144]).unwrap();
        #[cfg(unix)]
        let allocated_before = {
            use std::os::unix::fs::MetadataExt;
            file.sync_all().unwrap();
            file.metadata().unwrap().blocks()
        };

        discard_range(&file, high_start + 65_553, 131_163).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let allocated_after = file.metadata().unwrap().blocks();
            assert!(allocated_after < allocated_before);
            eprintln!("above 4 GiB allocated blocks: {allocated_before} -> {allocated_after}");
        }
        assert_eq!(file.metadata().unwrap().len(), file_length);
        file.seek(SeekFrom::Start(0)).unwrap();
        let mut bytes = vec![0; 262_144];
        file.read_exact(&mut bytes).unwrap();
        assert!(bytes.iter().all(|b| *b == 0x3c));
        file.seek(SeekFrom::Start(high_start)).unwrap();
        file.read_exact(&mut bytes).unwrap();
        assert!(bytes[..65_553].iter().all(|b| *b == 0x5a));
        assert!(bytes[65_553..196_716].iter().all(|b| *b == 0));
        assert!(bytes[196_716..].iter().all(|b| *b == 0x5a));
    }

    #[cfg(unix)]
    #[test]
    fn reclaim_sparse_range_releases_allocated_blocks() {
        use std::os::unix::fs::MetadataExt;
        let (mut file, _path) = tempfile::NamedTempFile::new().unwrap().into_parts();
        file.write_all(&vec![0x5a; 1024 * 1024]).unwrap();
        file.sync_all().unwrap();
        let before = file.metadata().unwrap().blocks();
        assert!(before > 0);

        discard_range(&file, 256 * 1024 + 17, 512 * 1024 + 91).unwrap();

        let after = file.metadata().unwrap().blocks();
        assert!(after < before, "allocated blocks: {before} -> {after}");
        eprintln!("allocated blocks: {before} -> {after}");
        assert_eq!(file.metadata().unwrap().len(), 1024 * 1024);
    }
}
