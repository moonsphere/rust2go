// Copyright 2024 ihciah. All Rights Reserved.

use std::{
    io::{Error, ErrorKind},
    os::fd::{AsRawFd, FromRawFd, RawFd},
    os::unix::io::OwnedFd,
};

use tokio::io::unix::AsyncFd;

pub(crate) fn new_pair() -> Result<(RawFd, RawFd), Error> {
    // create unix stream pair
    let mut fds = [-1; 2];
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "illumos",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "linux"
    ))]
    let flag = libc::SOCK_STREAM | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC;
    #[cfg(any(target_os = "ios", target_os = "macos"))]
    let flag = libc::SOCK_STREAM;

    macro_rules! ret_last_error {
        ($fn: ident ( $($arg: expr),* $(,)* )) => {
            if ::libc::$fn($($arg, )*) == -1 {
                return Err(Error::last_os_error());
            }
        };
        ($fn: ident ( $($arg: expr),* $(,)* ), $fds: ident) => {
            if ::libc::$fn($($arg, )*) == -1 {
                for fd in $fds {
                    ::libc::close(fd);
                }
                return Err(Error::last_os_error());
            }
        };
    }

    unsafe {
        ret_last_error!(socketpair(libc::AF_UNIX, flag, 0, fds.as_mut_ptr()));
        #[cfg(target_vendor = "apple")]
        for fd in fds.iter() {
            ret_last_error!(
                setsockopt(
                    *fd,
                    libc::SOL_SOCKET,
                    libc::SO_NOSIGPIPE,
                    &1 as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as u32,
                ),
                fds
            );
        }

        #[cfg(any(target_os = "ios", target_os = "macos"))]
        for fd in fds.iter() {
            ret_last_error!(fcntl(*fd, libc::F_SETFL, libc::O_NONBLOCK), fds);
        }
    }

    Ok((fds[0], fds[1]))
}

#[allow(unused)]
pub(crate) fn dup(fd: RawFd) -> Result<RawFd, Error> {
    let fd = unsafe { libc::dup(fd) };
    if fd == -1 {
        return Err(Error::last_os_error());
    }
    Ok(fd)
}

pub(crate) struct Notifier {
    fd: RawFd,
}

pub(crate) struct Awaiter {
    async_fd: AsyncFd<OwnedFd>,
}

impl AsRawFd for Notifier {
    fn as_raw_fd(&self) -> RawFd {
        self.fd
    }
}

impl AsRawFd for Awaiter {
    fn as_raw_fd(&self) -> RawFd {
        self.async_fd.get_ref().as_raw_fd()
    }
}

impl Notifier {
    #[allow(unused)]
    pub(crate) fn new() -> Result<(Self, RawFd), Error> {
        let (fd, peer) = new_pair()?;
        Ok((Self { fd }, peer))
    }

    #[inline]
    pub(crate) unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self { fd }
    }

    pub(crate) fn notify(&self) -> Result<(), Error> {
        const DATA: u8 = 0;
        loop {
            if unsafe { libc::write(self.fd, &DATA as *const u8 as *const libc::c_void, 1) } != -1 {
                return Ok(());
            }

            let err = Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
    }
}

impl Awaiter {
    #[allow(unused)]
    pub(crate) fn new() -> Result<(Self, RawFd), Error> {
        let (fd, peer) = new_pair()?;
        Ok((unsafe { Self::from_raw_fd(fd) }?, peer))
    }

    pub(crate) unsafe fn from_raw_fd(fd: RawFd) -> Result<Self, Error> {
        let owned = OwnedFd::from_raw_fd(fd);
        let async_fd = AsyncFd::new(owned)?;
        Ok(Self { async_fd })
    }

    pub(crate) async fn wait(&mut self) {
        loop {
            let mut readiness = match self.async_fd.readable_mut().await {
                Ok(r) => r,
                Err(_) => return,
            };
            let result = readiness.try_io(|inner| {
                let fd = inner.get_ref().as_raw_fd();
                let mut buf: [u8; 64] = [0; 64];
                loop {
                    let read_bytes =
                        unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
                    if read_bytes >= 0 {
                        return Ok(());
                    }
                    let err = Error::last_os_error();
                    if err.kind() == ErrorKind::Interrupted {
                        continue;
                    }
                    if err.kind() == ErrorKind::WouldBlock {
                        return Err(err);
                    }
                    return Ok(());
                }
            });
            match result {
                Ok(_) => return,
                Err(_would_block) => continue,
            }
        }
    }
}

impl Drop for Notifier {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.fd);
        }
    }
}
