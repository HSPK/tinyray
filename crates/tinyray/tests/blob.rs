#![cfg(target_os = "linux")]

use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::MetadataExt;
use std::time::Duration;
use tinyray::{
    decode_msgpack, decode_with_blob_limits, BlobDecodeLimits, BlobError, BlobRef,
    DEFAULT_MAX_BLOB_BYTES, MAX_DECODED_BLOB_HANDLES,
};

#[test]
fn blob_sizes_map_read_only_and_serde_as_the_reserved_extension() {
    for size in [64 << 10, 1 << 20, 16 << 20] {
        let data = (0..size)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let blob = BlobRef::from_bytes(&data).unwrap();
        assert_eq!(blob.len(), size);
        assert_eq!(blob.as_slice().unwrap(), data);

        let descriptor = blob.descriptor_bytes().unwrap();
        let received = BlobRef::open_descriptor(&descriptor).unwrap();
        assert_eq!(received.as_slice().unwrap(), data);

        let encoded = rmp_serde::to_vec(&blob).unwrap();
        let decoded: BlobRef = rmp_serde::from_slice(&encoded).unwrap();
        assert_eq!(decoded.as_slice().unwrap(), data);

        let seals = unsafe { libc::fcntl(blob.owner_fd().unwrap(), libc::F_GET_SEALS) };
        let required =
            libc::F_SEAL_GROW | libc::F_SEAL_SHRINK | libc::F_SEAL_WRITE | libc::F_SEAL_SEAL;
        assert_eq!(seals & required, required);
        let byte = [0u8; 1];
        assert_eq!(
            unsafe {
                libc::pwrite(
                    blob.owner_fd().unwrap(),
                    byte.as_ptr().cast(),
                    byte.len(),
                    32,
                )
            },
            -1
        );
    }
}

#[test]
fn receiver_mapping_survives_sender_close_but_unopened_descriptor_does_not() {
    let mut sender = BlobRef::from_bytes(b"retained").unwrap();
    let descriptor = sender.descriptor_bytes().unwrap();
    let receiver = BlobRef::open_descriptor(&descriptor).unwrap();
    let forwarded_descriptor = receiver.descriptor_bytes().unwrap();
    sender.close();
    assert_eq!(receiver.as_slice().unwrap(), b"retained");
    let forwarded = BlobRef::open_descriptor(&forwarded_descriptor).unwrap();
    assert_eq!(forwarded.as_slice().unwrap(), b"retained");

    let stale = BlobRef::from_bytes(b"stale").unwrap();
    let stale_descriptor = stale.descriptor_bytes().unwrap();
    drop(stale);
    assert!(matches!(
        BlobRef::open_descriptor(&stale_descriptor),
        Err(BlobError::Stale(_))
    ));
}

#[test]
fn descriptor_identity_and_size_are_checked_before_mapping() {
    let blob = BlobRef::from_bytes(b"verified").unwrap();
    let mut descriptor = blob.descriptor().unwrap();

    descriptor.boot[0] ^= 1;
    assert!(matches!(
        BlobRef::open_descriptor(&rmp_serde::to_vec_named(&descriptor).unwrap()),
        Err(BlobError::DifferentHost)
    ));
    descriptor = blob.descriptor().unwrap();

    descriptor.inode ^= 1;
    assert!(matches!(
        BlobRef::open_descriptor(&rmp_serde::to_vec_named(&descriptor).unwrap()),
        Err(BlobError::Stale(_))
    ));
    descriptor = blob.descriptor().unwrap();

    descriptor.device ^= 1;
    assert!(matches!(
        BlobRef::open_descriptor(&rmp_serde::to_vec_named(&descriptor).unwrap()),
        Err(BlobError::Stale(_))
    ));
    descriptor = blob.descriptor().unwrap();

    descriptor.token[0] ^= 1;
    assert!(matches!(
        BlobRef::open_descriptor(&rmp_serde::to_vec_named(&descriptor).unwrap()),
        Err(BlobError::Stale(_))
    ));
    descriptor = blob.descriptor().unwrap();

    descriptor.fd = i32::MAX;
    assert!(matches!(
        BlobRef::open_descriptor(&rmp_serde::to_vec_named(&descriptor).unwrap()),
        Err(BlobError::Stale(_))
    ));
    descriptor = blob.descriptor().unwrap();

    descriptor.size = DEFAULT_MAX_BLOB_BYTES as u64 + 1;
    assert!(matches!(
        BlobRef::open_descriptor(&rmp_serde::to_vec_named(&descriptor).unwrap()),
        Err(BlobError::TooLarge { .. })
    ));
    let mut descriptor_with_trailing = blob.descriptor_bytes().unwrap();
    descriptor_with_trailing.push(0);
    assert!(matches!(
        BlobRef::open_descriptor(&descriptor_with_trailing),
        Err(BlobError::Codec(_))
    ));
    assert!(tinyray::decode_msgpack::<u8>(&[1, 2]).is_err());

    let path = std::env::temp_dir().join(format!("tinyray-unsealed-{}", std::process::id()));
    let payload = b"unsealed";
    let mut contents = Vec::with_capacity(32 + payload.len());
    contents.extend_from_slice(b"TRBLOB01");
    contents.extend_from_slice(&descriptor.token);
    contents.extend_from_slice(&(payload.len() as u64).to_be_bytes());
    contents.extend_from_slice(payload);
    std::fs::write(&path, contents).unwrap();
    let file = std::fs::File::open(&path).unwrap();
    let metadata = file.metadata().unwrap();
    descriptor = blob.descriptor().unwrap();
    descriptor.fd = file.as_raw_fd();
    descriptor.size = payload.len() as u64;
    descriptor.device = metadata.dev();
    descriptor.inode = metadata.ino();
    assert!(matches!(
        BlobRef::open_descriptor(&rmp_serde::to_vec_named(&descriptor).unwrap()),
        Err(BlobError::Permission(_))
    ));
    std::fs::remove_file(path).unwrap();
}

#[test]
fn decode_limits_and_descriptor_dedup_bound_one_message() {
    let first = BlobRef::from_bytes(b"first").unwrap();
    let second = BlobRef::from_bytes(b"second").unwrap();

    let duplicate_wire = rmp_serde::to_vec(&vec![&first, &first, &first]).unwrap();
    let duplicate: Vec<BlobRef> = decode_msgpack(&duplicate_wire).unwrap();
    assert_eq!(duplicate.len(), 3);
    assert_eq!(
        duplicate[0].owner_fd().unwrap(),
        duplicate[1].owner_fd().unwrap()
    );
    assert_eq!(
        duplicate[1].owner_fd().unwrap(),
        duplicate[2].owner_fd().unwrap()
    );

    let too_many_wire = rmp_serde::to_vec(&vec![&first; 512]).unwrap();
    let too_many = decode_msgpack::<Vec<BlobRef>>(&too_many_wire).unwrap_err();
    assert!(too_many.to_string().contains("BlobRef values"));

    let aggregate_wire = rmp_serde::to_vec(&vec![&first, &second]).unwrap();
    let aggregate = decode_with_blob_limits::<Vec<BlobRef>>(
        &aggregate_wire,
        BlobDecodeLimits {
            max_refs: 2,
            max_mapped_bytes: 32 + first.len() + 32 + second.len() - 1,
        },
    )
    .unwrap_err();
    assert!(aggregate.to_string().contains("aggregate limit"));
}

#[test]
fn direct_serde_decode_has_a_process_resource_bound() {
    let source = BlobRef::from_bytes(b"bounded").unwrap();
    let wire = rmp_serde::to_vec(&vec![&source; MAX_DECODED_BLOB_HANDLES + 1]).unwrap();
    let decoded = rmp_serde::from_slice::<Vec<BlobRef>>(&wire).unwrap_err();
    assert!(decoded.to_string().contains("process limit"));
}

#[test]
fn getrandom_tokens_are_unique_across_fork() {
    let mut pipe = [0; 2];
    assert_eq!(
        unsafe { libc::pipe2(pipe.as_mut_ptr(), libc::O_CLOEXEC) },
        0
    );
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0);
    if pid == 0 {
        unsafe {
            libc::close(pipe[0]);
        }
        let child = BlobRef::from_bytes(b"child").unwrap();
        let token = child.descriptor().unwrap().token;
        let mut writer = unsafe { std::fs::File::from_raw_fd(pipe[1]) };
        writer.write_all(&token).unwrap();
        writer.flush().unwrap();
        unsafe { libc::_exit(0) };
    }
    unsafe {
        libc::close(pipe[1]);
    }
    let parent = BlobRef::from_bytes(b"parent").unwrap();
    let mut child_token = [0u8; 16];
    let mut reader = unsafe { std::fs::File::from_raw_fd(pipe[0]) };
    reader.read_exact(&mut child_token).unwrap();
    let mut status = 0;
    unsafe {
        libc::waitpid(pid, &mut status, 0);
    }
    assert_ne!(parent.descriptor().unwrap().token, child_token);
}

#[test]
fn file_creation_and_clone_close_semantics_are_explicit() {
    let path = std::env::temp_dir().join(format!("tinyray-blob-{}", std::process::id()));
    std::fs::write(&path, b"from-file").unwrap();
    let mut file = std::fs::File::open(&path).unwrap();
    file.seek(SeekFrom::Start(4)).unwrap();
    let mut first = BlobRef::from_file(&file).unwrap();
    assert_eq!(file.stream_position().unwrap(), 4);
    let second = first.clone();
    first.close();
    assert!(first.is_closed());
    assert_eq!(second.as_slice().unwrap(), b"from-file");
    std::fs::remove_file(path).unwrap();
}

#[test]
fn forked_child_close_does_not_cross_close_parent_mapping() {
    let blob = BlobRef::from_bytes(b"parent").unwrap();
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0);
    if pid == 0 {
        unsafe {
            libc::close(blob.owner_fd().unwrap());
        }
        unsafe { libc::_exit(0) };
    }
    let mut status = 0;
    unsafe {
        libc::waitpid(pid, &mut status, 0);
    }
    assert_eq!(blob.as_slice().unwrap(), b"parent");
}

#[test]
fn forked_child_serializes_its_own_pid_and_fd() {
    let blob = BlobRef::from_bytes(b"child-owned").unwrap();
    let mut ready = [0; 2];
    let mut forwarded = [0; 2];
    assert_eq!(unsafe { libc::pipe(ready.as_mut_ptr()) }, 0);
    assert_eq!(unsafe { libc::pipe(forwarded.as_mut_ptr()) }, 0);
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0);
    if pid == 0 {
        unsafe {
            libc::close(ready[1]);
            libc::close(forwarded[0]);
        }
        let mut signal = [0u8; 1];
        let mut reader = unsafe { std::fs::File::from_raw_fd(ready[0]) };
        reader.read_exact(&mut signal).unwrap();
        let descriptor = blob.descriptor_bytes().unwrap();
        let decoded: tinyray::BlobDescriptor = rmp_serde::from_slice(&descriptor).unwrap();
        assert_eq!(decoded.owner_pid, std::process::id());
        let mut writer = unsafe { std::fs::File::from_raw_fd(forwarded[1]) };
        writer
            .write_all(&(descriptor.len() as u32).to_be_bytes())
            .unwrap();
        writer.write_all(&descriptor).unwrap();
        writer.flush().unwrap();
        std::thread::sleep(Duration::from_millis(200));
        unsafe { libc::_exit(0) };
    }
    unsafe {
        libc::close(ready[0]);
        libc::close(forwarded[1]);
    }
    drop(blob);
    let mut signal = unsafe { std::fs::File::from_raw_fd(ready[1]) };
    signal.write_all(&[1]).unwrap();
    let mut reader = unsafe { std::fs::File::from_raw_fd(forwarded[0]) };
    let mut prefix = [0u8; 4];
    reader.read_exact(&mut prefix).unwrap();
    let mut descriptor = vec![0; u32::from_be_bytes(prefix) as usize];
    reader.read_exact(&mut descriptor).unwrap();
    let opened = BlobRef::open_descriptor(&descriptor).unwrap();
    assert_eq!(opened.as_slice().unwrap(), b"child-owned");
    let mut status = 0;
    unsafe {
        libc::waitpid(pid, &mut status, 0);
    }
}

#[test]
fn owner_exit_invalidates_unopened_descriptor_but_not_an_open_mapping() {
    let mut pipe = [0; 2];
    assert_eq!(
        unsafe { libc::pipe2(pipe.as_mut_ptr(), libc::O_CLOEXEC) },
        0
    );
    let pid = unsafe { libc::fork() };
    assert!(pid >= 0);
    if pid == 0 {
        unsafe {
            libc::close(pipe[0]);
        }
        let blob = BlobRef::from_bytes(b"crash-safe").unwrap();
        let descriptor = blob.descriptor_bytes().unwrap();
        let mut writer = unsafe { std::fs::File::from_raw_fd(pipe[1]) };
        writer
            .write_all(&(descriptor.len() as u32).to_be_bytes())
            .unwrap();
        writer.write_all(&descriptor).unwrap();
        writer.flush().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        unsafe { libc::_exit(0) };
    }
    unsafe {
        libc::close(pipe[1]);
    }
    let mut reader = unsafe { std::fs::File::from_raw_fd(pipe[0]) };
    let mut prefix = [0u8; 4];
    reader.read_exact(&mut prefix).unwrap();
    let mut descriptor = vec![0; u32::from_be_bytes(prefix) as usize];
    reader.read_exact(&mut descriptor).unwrap();
    let opened = BlobRef::open_descriptor(&descriptor).unwrap();
    let mut status = 0;
    unsafe {
        libc::waitpid(pid, &mut status, 0);
    }
    assert_eq!(opened.as_slice().unwrap(), b"crash-safe");
    assert!(matches!(
        BlobRef::open_descriptor(&descriptor),
        Err(BlobError::Stale(_))
    ));
}
