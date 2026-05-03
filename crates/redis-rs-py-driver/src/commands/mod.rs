// Per-family command modules.
//
// Each file holds `#[pymethods] impl Redis` and `impl AsyncRedis` blocks adding that
// family's commands. PyO3 0.28 supports multiple `#[pymethods]` blocks
// per class as long as method names are unique across blocks.
//
// New families append a `pub mod <family>;` line below.

pub mod admin;
pub mod hashes;
pub mod lists;
pub mod scripts;
pub mod sets;
pub mod streams;
pub mod strings;
pub mod zsets;
