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
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let result = unsafe {
            nix::libc::fallocate(
                file.as_raw_fd(),
                nix::libc::FALLOC_FL_PUNCH_HOLE | nix::libc::FALLOC_FL_KEEP_SIZE,
                offset.try_into()?,
                length.try_into()?,
            )
        };
        if result != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        file.sync_all()?;
        Ok(())
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    anyhow::bail!("physical range reclamation is unsupported on this platform")
}

#[cfg(all(test, windows))]
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
        assert_eq!(std::fs::read(external).unwrap(), vec![0x5a; 128 * 1024]);
    }

    #[test]
    fn reclaim_sparse_range_preserves_length_and_neighbour_bytes() {
        let mut file = tempfile::tempfile().unwrap();
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
}
