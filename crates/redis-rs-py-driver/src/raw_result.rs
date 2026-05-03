// IntoRawResult trait + From<T> for RawResult impls.
//
// Lets command bodies write `.await.into_raw_result()` regardless of the
// concrete redis-rs return type. Each typed return needs:
//   1. A RawResult variant (in async_bridge.rs).
//   2. A `From<T> for RawResult` impl here (so IntoRawResult covers it).
//   3. (Optional) A sync py_* helper in driver.rs for the matching sync method.

use crate::async_bridge::RawResult;
use crate::errors::classify;

pub trait IntoRawResult {
    fn into_raw_result(self) -> RawResult;
}

impl<T> IntoRawResult for redis::RedisResult<T>
where
    T: Into<RawResult>,
{
    fn into_raw_result(self) -> RawResult {
        match self {
            Ok(v) => v.into(),
            Err(e) => classify(e),
        }
    }
}

impl From<()> for RawResult {
    fn from(_: ()) -> Self {
        RawResult::Nil
    }
}

impl From<bool> for RawResult {
    fn from(v: bool) -> Self {
        RawResult::Bool(v)
    }
}

impl From<i64> for RawResult {
    fn from(v: i64) -> Self {
        RawResult::Int(v)
    }
}

impl From<u64> for RawResult {
    fn from(v: u64) -> Self {
        // u64 → i64 truncating cast is fine: redis returns signed counts and
        // u64 returns from EXISTS/DEL fit in i64 range in any realistic setup.
        RawResult::Int(v as i64)
    }
}

impl From<f64> for RawResult {
    fn from(v: f64) -> Self {
        RawResult::F64(v)
    }
}

impl From<Option<i64>> for RawResult {
    fn from(v: Option<i64>) -> Self {
        RawResult::OptInt(v)
    }
}

impl From<Option<f64>> for RawResult {
    fn from(v: Option<f64>) -> Self {
        RawResult::OptF64(v)
    }
}

impl From<Vec<u8>> for RawResult {
    fn from(v: Vec<u8>) -> Self {
        RawResult::OptBytes(Some(v))
    }
}

impl From<Option<Vec<u8>>> for RawResult {
    fn from(v: Option<Vec<u8>>) -> Self {
        RawResult::OptBytes(v)
    }
}

impl From<String> for RawResult {
    fn from(v: String) -> Self {
        RawResult::Str(v)
    }
}

impl From<Option<String>> for RawResult {
    fn from(v: Option<String>) -> Self {
        RawResult::OptStr(v)
    }
}

impl From<Vec<Vec<u8>>> for RawResult {
    fn from(v: Vec<Vec<u8>>) -> Self {
        RawResult::BytesList(v)
    }
}

impl From<Vec<Option<Vec<u8>>>> for RawResult {
    fn from(v: Vec<Option<Vec<u8>>>) -> Self {
        RawResult::OptBytesList(v)
    }
}

impl From<Vec<String>> for RawResult {
    fn from(v: Vec<String>) -> Self {
        RawResult::StringList(v)
    }
}

impl From<Vec<(Vec<u8>, Vec<u8>)>> for RawResult {
    fn from(v: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        RawResult::BytesPairs(v)
    }
}

impl From<Vec<(Vec<u8>, f64)>> for RawResult {
    fn from(v: Vec<(Vec<u8>, f64)>) -> Self {
        RawResult::ScoredMembers(v)
    }
}

impl From<Option<(String, Vec<u8>)>> for RawResult {
    fn from(v: Option<(String, Vec<u8>)>) -> Self {
        RawResult::OptKeyAndBytes(v)
    }
}

impl From<Option<(String, Vec<Vec<u8>>)>> for RawResult {
    fn from(v: Option<(String, Vec<Vec<u8>>)>) -> Self {
        RawResult::OptKeyAndBytesList(v)
    }
}

impl From<(u64, Vec<String>)> for RawResult {
    fn from(v: (u64, Vec<String>)) -> Self {
        RawResult::CursorAndStrings(v.0, v.1)
    }
}

impl From<Vec<i64>> for RawResult {
    fn from(v: Vec<i64>) -> Self {
        RawResult::IntList(v)
    }
}

impl From<redis::Value> for RawResult {
    fn from(v: redis::Value) -> Self {
        RawResult::Value(v)
    }
}

impl From<Vec<bool>> for RawResult {
    fn from(v: Vec<bool>) -> Self {
        RawResult::BoolList(v)
    }
}

// Stream From<T> impls (Plan 08)
impl From<Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>> for RawResult {
    fn from(v: Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>) -> Self {
        RawResult::StreamEntries(v)
    }
}

impl From<Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>)>> for RawResult {
    fn from(v: Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>)>)>) -> Self {
        RawResult::StreamReadEntries(v)
    }
}

impl From<Option<(i64, Vec<u8>, Vec<u8>, Vec<(Vec<u8>, i64)>)>> for RawResult {
    fn from(v: Option<(i64, Vec<u8>, Vec<u8>, Vec<(Vec<u8>, i64)>)>) -> Self {
        RawResult::StreamPendingSummary(v)
    }
}

impl From<Vec<(Vec<u8>, Vec<u8>, i64, i64)>> for RawResult {
    fn from(v: Vec<(Vec<u8>, Vec<u8>, i64, i64)>) -> Self {
        RawResult::StreamPendingRange(v)
    }
}
