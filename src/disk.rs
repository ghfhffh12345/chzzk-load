use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DiskSpace {
    pub available_bytes: u64,
    pub total_bytes: u64,
}

impl DiskSpace {
    pub fn available_gb(&self) -> f64 {
        self.available_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }

    pub fn total_gb(&self) -> f64 {
        self.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0)
    }
}

/// Retrieves available and total disk space for the volume containing the specified path.
/// If the path does not exist, it traverses parent directories until an existing ancestor is found.
#[cfg(windows)]
pub fn get_disk_space<P: AsRef<Path>>(path: P) -> std::io::Result<DiskSpace> {
    use std::os::windows::ffi::OsStrExt;

    let mut current = path.as_ref();
    while !current.exists() {
        if let Some(parent) = current.parent() {
            if parent.as_os_str().is_empty() {
                break;
            }
            current = parent;
        } else {
            break;
        }
    }

    let query_path = if current.exists() {
        current
    } else {
        Path::new(".")
    };
    let abs_path = std::fs::canonicalize(query_path).unwrap_or_else(|_| query_path.to_path_buf());

    let mut wide: Vec<u16> = abs_path.as_os_str().encode_wide().collect();
    wide.push(0);

    let mut free_bytes_available_to_caller = 0u64;
    let mut total_number_of_bytes = 0u64;
    let mut total_number_of_free_bytes = 0u64;

    let success = unsafe {
        windows_sys::Win32::Storage::FileSystem::GetDiskFreeSpaceExW(
            wide.as_ptr(),
            &mut free_bytes_available_to_caller,
            &mut total_number_of_bytes,
            &mut total_number_of_free_bytes,
        )
    };

    if success != 0 {
        Ok(DiskSpace {
            available_bytes: free_bytes_available_to_caller,
            total_bytes: total_number_of_bytes,
        })
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
pub fn get_disk_space<P: AsRef<Path>>(path: P) -> std::io::Result<DiskSpace> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let mut current = path.as_ref();
    while !current.exists() {
        if let Some(parent) = current.parent() {
            if parent.as_os_str().is_empty() {
                break;
            }
            current = parent;
        } else {
            break;
        }
    }

    let query_path = if current.exists() {
        current
    } else {
        Path::new(".")
    };
    let abs_path = std::fs::canonicalize(query_path).unwrap_or_else(|_| query_path.to_path_buf());

    let c_path = CString::new(abs_path.as_os_str().as_bytes())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let res = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };

    if res == 0 {
        let block_size = if stat.f_frsize != 0 {
            stat.f_frsize as u64
        } else {
            stat.f_bsize as u64
        };
        let available_bytes = (stat.f_bavail as u64) * block_size;
        let total_bytes = (stat.f_blocks as u64) * block_size;

        Ok(DiskSpace {
            available_bytes,
            total_bytes,
        })
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(any(windows, unix)))]
pub fn get_disk_space<P: AsRef<Path>>(_path: P) -> std::io::Result<DiskSpace> {
    Ok(DiskSpace {
        available_bytes: u64::MAX,
        total_bytes: u64::MAX,
    })
}

/// Checks whether the available disk space for the given path meets or exceeds `min_free_disk_gb`.
/// Returns `true` if threshold is met or if `min_free_disk_gb <= 0.0`.
pub fn has_sufficient_disk_space<P: AsRef<Path>>(path: P, min_free_disk_gb: f64) -> bool {
    if min_free_disk_gb <= 0.0 {
        return true;
    }
    match get_disk_space(path) {
        Ok(space) => space.available_gb() >= min_free_disk_gb,
        Err(_) => true, // If unable to query disk space, do not falsely block recording
    }
}
