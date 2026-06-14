//! Content-addressed identifiers used by feeder plugins' `compute_outcomes`
//! to hash upstream-fetched bytes.
//!
//! Two function families live here:
//!
//! - [`compute_midhash256`] / [`compute_midhash256_from_sample`] —
//!   the size-prefix-plus-middle-1MB-sample identification primitive shared
//!   with `meta-sort` / `meta-share` / meta-core. Custom multicodec `0x1000`.
//!   Fast but not IPFS-interop. The `_from_sample` entrypoint takes the
//!   middle slice directly so plugins that can only fetch a partial range
//!   (BitTorrent BEP 9, sliced HTTP) don't have to buffer the whole file
//!   just to throw most of it away. **Rust-only by construction.**
//! - [`compute_ipfs_cid`] is the standard IPFS CIDv1 produced over the full
//!   bytes — raw codec `0x55` for a single-chunk file, UnixFS dag-pb codec
//!   `0x70` rooted on chunked raw-codec leaves for larger files. Output matches
//!   `ipfs add --cid-version=1 --raw-leaves=true --chunker=size-262144
//!   --hash=sha2-256 <file>`.
//!
//! Cross-implementation parity for `compute_midhash256` with the TypeScript
//! `FastHash.ts` is load-bearing — keep the fixture tests pinned, a regression
//! silently breaks dedupe across the platform.

use bytes::Bytes;
use sha2::{Digest, Sha256};

/// Sample window in bytes (1 MiB). Files this size or smaller are hashed
/// in full; larger files are hashed over the middle 1 MiB slice.
const SAMPLE_SIZE: usize = 1024 * 1024;

/// Per-block IPFS chunk size (256 KiB), matches kubo's
/// `--chunker=size-262144` default.
pub const IPFS_CHUNK_SIZE: usize = 256 * 1024;

/// dag-pb fanout — children per internal node. Matches kubo's balanced
/// builder default.
pub const IPFS_FANOUT: usize = 174;

/// Output of [`compute_ipfs_blocks`]. Carries the root CID **plus every
/// intermediate block** (leaf chunks AND internal dag-pb nodes) keyed by
/// their own CID. Pass `.blocks` to the gateway's blockstore so peers
/// can fetch the file by CID via bitswap.
#[derive(Debug, Clone)]
pub struct IpfsBlocks {
    /// Canonical "bafy…" / "bafk…" root cid string. Identical to what
    /// [`compute_ipfs_cid`] returns for the same input.
    pub root: String,
    /// Every block — root included — keyed by its own cid. For a
    /// single-leaf file, this is a one-entry vec `[(root, payload)]`
    /// (raw codec, payload is the file bytes). For multi-leaf files,
    /// it is `leaves ++ internal_nodes` in build order; the **last**
    /// entry is the root.
    pub blocks: Vec<(String, Bytes)>,
}

/// Compute a midhash256 CID matching the TypeScript implementation in
/// `meta-hash/src/lib/file-id/FastHash.ts`:
///
/// 1. Build the hash input as `[size:u64-be][middle 1 MiB sample]` (or the
///    whole file if it fits in 1 MiB).
/// 2. SHA-256 it.
/// 3. Wrap in CIDv1 with custom multicodec `0x1000` for both the codec field
///    and the multihash hash-code field, length 32, then multibase-base32-lower
///    with the `b` prefix.
///
/// **Endianness for the size prefix is big-endian** to match TS's
/// `writeBigUInt64BE`. Cross-impl mismatch here would silently mangle every
/// import — keep the test pinned.
pub fn compute_midhash256(bytes: &[u8]) -> String {
    let size = bytes.len();
    let sample: &[u8] = if size <= SAMPLE_SIZE {
        bytes
    } else {
        let start = (size - SAMPLE_SIZE) / 2;
        &bytes[start..start + SAMPLE_SIZE]
    };
    compute_midhash256_from_sample(size as u64, sample)
}

/// Same midhash256 primitive as [`compute_midhash256`], but takes the
/// sample bytes directly rather than slicing them out of a full file.
/// For upstreams that can only deliver a middle range (BitTorrent BEP 9,
/// sliced HTTP), this avoids forcing the caller to buffer the whole file
/// in memory just to throw most of it away.
///
/// `total_size` is the **full file size in bytes** — used as the
/// size prefix, NOT `middle_sample.len()`.
pub fn compute_midhash256_from_sample(total_size: u64, middle_sample: &[u8]) -> String {
    let size_be = total_size.to_be_bytes();
    let mut hasher = Sha256::new();
    hasher.update(size_be);
    hasher.update(middle_sample);
    let digest: [u8; 32] = hasher.finalize().into();

    // Multicodec 0x1000 encoded as unsigned varint = [0x80, 0x20].
    // CIDv1 layout: [version=0x01][codec varint][multihash code varint][len][digest]
    let mut wire = Vec::with_capacity(38);
    wire.push(0x01);
    wire.extend_from_slice(&[0x80, 0x20]);
    wire.extend_from_slice(&[0x80, 0x20]);
    wire.push(0x20);
    wire.extend_from_slice(&digest);

    format!("b{}", base32_lower_no_padding(&wire))
}

/// Wrap a 20-byte BitTorrent v1 info-hash as a multibase CIDv1.
///
/// BT v1 info-hashes ARE SHA-1 digests of the bencoded info dict — so
/// the natural CID form is `codec=raw (0x55)` + `multihash=sha1 (0x11),
/// len=20, digest=<infohash bytes>`.
pub fn compute_bt_info_cid(infohash_20: &[u8; 20]) -> String {
    // CIDv1 layout: [version=0x01][codec varint][multihash code varint][len][digest]
    let mut wire = Vec::with_capacity(4 + 20);
    wire.push(0x01); // CIDv1 version
    wire.push(0x55); // codec: raw
    wire.push(0x11); // multihash: sha1
    wire.push(0x14); // multihash length: 20
    wire.extend_from_slice(infohash_20);
    format!("b{}", base32_lower_no_padding(&wire))
}

/// Custom multicodec for a single file inside a **BitTorrent v1** torrent
/// (`btih-v1-file`). MetaMesh-private, adjacent to midhash256's `0x1000`.
pub const BTIH_V1_FILE_CODEC: u64 = 0x1001;

/// Custom multicodec for a single file inside a **BitTorrent v2** torrent
/// (`btih-v2-file`).
pub const BTIH_V2_FILE_CODEC: u64 = 0x1002;

/// Encode a `btih-v1-file` CID — a single file inside a BT v1 torrent,
/// addressed by the 20-byte v1 infohash (SHA-1 of the bencoded `info`
/// dict) plus the zero-based file index in `info.files`. Single-file
/// torrents use index `0`.
pub fn compute_bt_v1_file_cid(infohash_20: &[u8; 20], file_index: u64) -> String {
    bt_file_cid(BTIH_V1_FILE_CODEC, infohash_20, file_index)
}

/// Encode a `btih-v2-file` CID — a single file inside a BT v2 torrent
/// (BEP 52), addressed by the 32-byte v2 infohash plus the file's
/// canonical traversal index in `file tree`.
pub fn compute_bt_v2_file_cid(infohash_32: &[u8; 32], file_index: u64) -> String {
    bt_file_cid(BTIH_V2_FILE_CODEC, infohash_32, file_index)
}

/// Shared encoder for the two torrent-file CID families. `infohash` is 20
/// bytes for v1 / 32 for v2; `codec` selects the family.
fn bt_file_cid(codec: u64, infohash: &[u8], file_index: u64) -> String {
    // digest = infohash ‖ varint(file_index)
    let mut digest = Vec::with_capacity(infohash.len() + 2);
    digest.extend_from_slice(infohash);
    write_pb_varint(file_index, &mut digest);

    // CIDv1: [version][codec varint][multihash code varint][len varint][digest]
    let mut wire = Vec::with_capacity(1 + 4 + digest.len());
    wire.push(0x01);
    write_pb_varint(codec, &mut wire);
    write_pb_varint(codec, &mut wire);
    write_pb_varint(digest.len() as u64, &mut wire);
    wire.extend_from_slice(&digest);
    format!("b{}", base32_lower_no_padding(&wire))
}

/// Compute a standard IPFS CIDv1 over `bytes`. Output matches kubo's
/// `ipfs add --cid-version=1 --raw-leaves=true --chunker=size-262144
/// --hash=sha2-256 <file>`.
pub fn compute_ipfs_cid(bytes: &[u8]) -> String {
    compute_ipfs_blocks(bytes).root
}

/// Same wire-format guarantee as [`compute_ipfs_cid`], but exposes every
/// block produced along the way (leaf chunks + internal dag-pb nodes)
/// so callers can populate a bitswap blockstore in one pass.
pub fn compute_ipfs_blocks(bytes: &[u8]) -> IpfsBlocks {
    // Leaves: chunk bytes into raw-codec blocks. Empty input still gets
    // one (empty) leaf so the cid is well-defined.
    let mut blocks: Vec<(String, Bytes)> = Vec::new();
    let leaves: Vec<(Vec<u8>, u64)> = if bytes.is_empty() {
        let cid_wire = ipfs_cid_wire(0x55, &sha2_256(b""));
        blocks.push((cid_string(&cid_wire), Bytes::new()));
        vec![(cid_wire, 0)]
    } else {
        bytes
            .chunks(IPFS_CHUNK_SIZE)
            .map(|chunk| {
                let cid_wire = ipfs_cid_wire(0x55, &sha2_256(chunk));
                blocks.push((cid_string(&cid_wire), Bytes::copy_from_slice(chunk)));
                (cid_wire, chunk.len() as u64)
            })
            .collect()
    };

    // Single-leaf file: return the leaf cid as the file cid.
    if leaves.len() == 1 {
        let root = cid_string(&leaves[0].0);
        return IpfsBlocks { root, blocks };
    }

    // Multi-leaf: build a balanced tree of UnixFS dag-pb internal nodes.
    let mut level: Vec<(Vec<u8>, u64, u64)> = leaves
        .into_iter()
        .map(|(cid, size)| (cid, size, size))
        .collect();

    while level.len() > 1 {
        let mut next: Vec<(Vec<u8>, u64, u64)> =
            Vec::with_capacity(level.len().div_ceil(IPFS_FANOUT));
        for batch in level.chunks(IPFS_FANOUT) {
            let blocksizes: Vec<u64> = batch.iter().map(|(_, sz, _)| *sz).collect();
            let filesize: u64 = blocksizes.iter().sum();
            let unixfs_data = encode_unixfs_file(filesize, &blocksizes);
            let node_bytes = encode_dagpb_node(batch, &unixfs_data);
            let node_cid = ipfs_cid_wire(0x70, &sha2_256(&node_bytes));
            blocks.push((cid_string(&node_cid), Bytes::from(node_bytes.clone())));
            let own_tsize: u64 =
                batch.iter().map(|(_, _, t)| *t).sum::<u64>() + node_bytes.len() as u64;
            next.push((node_cid, filesize, own_tsize));
        }
        level = next;
    }
    let root = cid_string(&level[0].0);
    IpfsBlocks { root, blocks }
}

fn cid_string(wire: &[u8]) -> String {
    format!("b{}", base32_lower_no_padding(wire))
}

/// CIDv1 wire-form bytes: `[version=0x01][codec varint][multihash code
/// varint=0x12 sha2-256][len=0x20][digest...]`.
fn ipfs_cid_wire(codec: u8, digest: &[u8; 32]) -> Vec<u8> {
    debug_assert!(
        codec < 0x80,
        "codec varint must fit in one byte for this helper"
    );
    let mut wire = Vec::with_capacity(36);
    wire.push(0x01); // CIDv1
    wire.push(codec);
    wire.push(0x12); // multihash code: sha2-256
    wire.push(0x20); // digest length: 32
    wire.extend_from_slice(digest);
    wire
}

fn sha2_256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// LEB128 unsigned varint encoding into `out`.
fn write_pb_varint(value: u64, out: &mut Vec<u8>) {
    let mut v = value;
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn write_pb_varint_field(field: u32, value: u64, out: &mut Vec<u8>) {
    write_pb_varint((field as u64) << 3, out); // tag: (field << 3) | wire-type 0 (varint)
    write_pb_varint(value, out);
}

fn write_pb_bytes_field(field: u32, value: &[u8], out: &mut Vec<u8>) {
    write_pb_varint(((field as u64) << 3) | 2, out); // wire type 2 = length-delimited
    write_pb_varint(value.len() as u64, out);
    out.extend_from_slice(value);
}

/// UnixFS protobuf payload for a File node: `Type=File, filesize,
/// blocksizes[]`.
fn encode_unixfs_file(filesize: u64, blocksizes: &[u64]) -> Vec<u8> {
    let mut out = Vec::new();
    write_pb_varint_field(1, 2, &mut out); // Type = File
    write_pb_varint_field(3, filesize, &mut out); // filesize
    for &bs in blocksizes {
        write_pb_varint_field(4, bs, &mut out); // blocksizes (repeated)
    }
    out
}

/// dag-pb PBNode for an internal UnixFS file node. Canonical wire order
/// pinned by the dag-pb spec: `Links` (tag 2) first, `Data` (tag 1) second.
fn encode_dagpb_node(children: &[(Vec<u8>, u64, u64)], data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    for (cid_wire, _filesize, tsize) in children {
        let mut link_bytes = Vec::new();
        write_pb_bytes_field(1, cid_wire, &mut link_bytes); // Hash
        write_pb_varint_field(3, *tsize, &mut link_bytes); // Tsize
        write_pb_bytes_field(2, &link_bytes, &mut out); // Links (tag 2) first
    }
    write_pb_bytes_field(1, data, &mut out); // Data (tag 1) last
    out
}

/// RFC 4648 base32 with the lowercase alphabet and no padding. Used for the
/// multibase `b` prefix.
pub(crate) fn base32_lower_no_padding(input: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut out = String::with_capacity(input.len().div_ceil(5) * 8);
    let mut buffer: u64 = 0;
    let mut bits: u32 = 0;
    for &b in input {
        buffer = (buffer << 8) | b as u64;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            let idx = ((buffer >> bits) & 0x1F) as usize;
            out.push(ALPHABET[idx] as char);
        }
    }
    if bits > 0 {
        let idx = ((buffer << (5 - bits)) & 0x1F) as usize;
        out.push(ALPHABET[idx] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn midhash256_matches_ts_fixtures() {
        assert_eq!(
            compute_midhash256(b"hello world"),
            "bagacbabaec7v3fu2ygzh3e2sybg3fbzmisry2hbtpmck6vx3yftea6vzq35r4"
        );
        assert_eq!(
            compute_midhash256(b""),
            "bagacbabaecxvk4hvugaqw6xxrsxuxrykmyhq35i6ik5pshkn4wzdfdpa5a67y"
        );
        let mb_zeros = vec![0u8; 1024 * 1024];
        assert_eq!(
            compute_midhash256(&mb_zeros),
            "bagacbabaeawlkt4hn34sxuieoosrnnag3g3wv7gc6alzwfms2nrph2hochrx4"
        );
    }

    #[test]
    fn midhash256_from_sample_matches_full_bytes() {
        let small: &[u8] = b"hello world";
        assert_eq!(
            compute_midhash256_from_sample(small.len() as u64, small),
            compute_midhash256(small),
        );

        assert_eq!(
            compute_midhash256_from_sample(0, b""),
            compute_midhash256(b""),
        );

        let one_mb = vec![0u8; SAMPLE_SIZE];
        assert_eq!(
            compute_midhash256_from_sample(one_mb.len() as u64, &one_mb),
            compute_midhash256(&one_mb),
        );

        let two_mb = vec![0u8; 2 * SAMPLE_SIZE];
        let start = (two_mb.len() - SAMPLE_SIZE) / 2;
        let middle = &two_mb[start..start + SAMPLE_SIZE];
        assert_eq!(
            compute_midhash256_from_sample(two_mb.len() as u64, middle),
            compute_midhash256(&two_mb),
        );
    }

    #[test]
    fn midhash256_from_sample_partial_sample_stable() {
        let sample = vec![0u8; SAMPLE_SIZE];
        let total_5gb: u64 = 5 * 1024 * 1024 * 1024;
        let total_10gb: u64 = 10 * 1024 * 1024 * 1024;

        let h1 = compute_midhash256_from_sample(total_5gb, &sample);
        let h2 = compute_midhash256_from_sample(total_5gb, &sample);
        assert_eq!(h1, h2, "same (size, sample) must hash identically");

        let h3 = compute_midhash256_from_sample(total_10gb, &sample);
        assert_ne!(
            h1, h3,
            "size prefix must contribute to cid — same sample, different size"
        );
    }

    #[test]
    fn bt_v1_file_cid_wire_layout() {
        let infohash: [u8; 20] = [
            0xf9, 0xc8, 0xa7, 0xb6, 0xe5, 0xd4, 0xc3, 0xb2, 0xa1, 0x90, 0x8f, 0x7e, 0x6d, 0x5c,
            0x4b, 0x3a, 0x29, 0x18, 0x07, 0x06,
        ];

        let mut want = vec![0x01, 0x81, 0x20, 0x81, 0x20, 0x15];
        want.extend_from_slice(&infohash);
        want.push(0x04);
        assert_eq!(
            compute_bt_v1_file_cid(&infohash, 4),
            format!("b{}", base32_lower_no_padding(&want))
        );

        let mut want0 = vec![0x01, 0x81, 0x20, 0x81, 0x20, 0x15];
        want0.extend_from_slice(&infohash);
        want0.push(0x00);
        assert_eq!(
            compute_bt_v1_file_cid(&infohash, 0),
            format!("b{}", base32_lower_no_padding(&want0))
        );

        let mut want200 = vec![0x01, 0x81, 0x20, 0x81, 0x20, 0x16];
        want200.extend_from_slice(&infohash);
        want200.extend_from_slice(&[0xC8, 0x01]);
        assert_eq!(
            compute_bt_v1_file_cid(&infohash, 200),
            format!("b{}", base32_lower_no_padding(&want200))
        );
    }

    #[test]
    fn bt_v2_file_cid_wire_layout() {
        let infohash: [u8; 32] = [0xAB; 32];
        let mut want = vec![0x01, 0x82, 0x20, 0x82, 0x20, 0x21];
        want.extend_from_slice(&infohash);
        want.push(0x00);
        assert_eq!(
            compute_bt_v2_file_cid(&infohash, 0),
            format!("b{}", base32_lower_no_padding(&want))
        );
    }

    #[test]
    fn bt_file_cid_properties() {
        let ih1 = [0x11u8; 20];
        let ih2 = [0x22u8; 32];

        assert_eq!(
            compute_bt_v1_file_cid(&ih1, 3),
            compute_bt_v1_file_cid(&ih1, 3)
        );
        assert_ne!(
            compute_bt_v1_file_cid(&ih1, 0),
            compute_bt_v1_file_cid(&ih1, 1)
        );
        assert!(compute_bt_v1_file_cid(&ih1, 0).starts_with('b'));
        assert!(compute_bt_v2_file_cid(&ih2, 0).starts_with('b'));
        assert_ne!(
            compute_bt_v1_file_cid(&[0xAB; 20], 0),
            compute_bt_v2_file_cid(&[0xAB; 32], 0)
        );
    }

    #[test]
    fn base32_known_vectors() {
        assert_eq!(base32_lower_no_padding(b""), "");
        assert_eq!(base32_lower_no_padding(b"f"), "my");
        assert_eq!(base32_lower_no_padding(b"fo"), "mzxq");
        assert_eq!(base32_lower_no_padding(b"foo"), "mzxw6");
        assert_eq!(base32_lower_no_padding(b"foob"), "mzxw6yq");
        assert_eq!(base32_lower_no_padding(b"foobar"), "mzxw6ytboi");
    }

    #[test]
    fn ipfs_cid_single_leaf_matches_kubo() {
        assert_eq!(
            compute_ipfs_cid(b"hello world"),
            "bafkreifzjut3te2nhyekklss27nh3k72ysco7y32koao5eei66wof36n5e"
        );
    }

    #[test]
    fn ipfs_cid_empty_matches_kubo() {
        assert_eq!(
            compute_ipfs_cid(b""),
            "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
        );
    }

    #[test]
    fn ipfs_cid_multi_leaf_zeros_stable() {
        let mb_zeros = vec![0u8; 1024 * 1024];
        assert_eq!(
            compute_ipfs_cid(&mb_zeros),
            "bafybeiadh3bekpwtewjvauqeucf7yzqrb3ixsxzltnuwed4pxangtpou6m"
        );
    }

    #[test]
    fn ipfs_blocks_single_leaf() {
        let payload = b"hello world";
        let out = compute_ipfs_blocks(payload);
        assert_eq!(out.root, compute_ipfs_cid(payload));
        assert_eq!(out.blocks.len(), 1);
        assert_eq!(out.blocks[0].0, out.root);
        assert_eq!(out.blocks[0].1.as_ref(), payload);
    }

    #[test]
    fn ipfs_blocks_multi_leaf() {
        let mb_zeros = vec![0u8; 1024 * 1024];
        let out = compute_ipfs_blocks(&mb_zeros);
        assert_eq!(
            out.root,
            "bafybeiadh3bekpwtewjvauqeucf7yzqrb3ixsxzltnuwed4pxangtpou6m"
        );
        assert_eq!(out.blocks.len(), 5);
        assert_eq!(out.blocks.last().unwrap().0, out.root);
        for (_cid, block) in &out.blocks[..4] {
            assert_eq!(block.len(), 256 * 1024);
            assert!(block.iter().all(|&b| b == 0));
        }
    }

    #[test]
    fn ipfs_blocks_empty() {
        let out = compute_ipfs_blocks(b"");
        assert_eq!(
            out.root,
            "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
        );
        assert_eq!(out.blocks.len(), 1);
        assert_eq!(out.blocks[0].1.len(), 0);
        assert_eq!(out.blocks[0].0, out.root);
    }
}
