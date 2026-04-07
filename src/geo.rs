/// WGS84 bounds — the limits of the Mercator projection.
pub const LON_MIN: f64 = -180.0;
pub const LON_MAX: f64 = 180.0;
pub const LAT_MIN: f64 = -85.05112878;
pub const LAT_MAX: f64 = 85.05112878;

/// 26 bits per axis → 52-bit interleaved hash stored as f64.
/// f64 can represent integers exactly up to 2^53, so 52 bits fit perfectly.
const STEPS: u64 = 1 << 26;

const EARTH_RADIUS_M: f64 = 6372797.560856;

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

pub fn validate(lon: f64, lat: f64) -> bool {
    (LON_MIN..=LON_MAX).contains(&lon) && (LAT_MIN..=LAT_MAX).contains(&lat)
}

// ---------------------------------------------------------------------------
// Encode / decode
// ---------------------------------------------------------------------------

/// Encodes (lon, lat) into a 52-bit geohash returned as f64.
pub fn encode(lon: f64, lat: f64) -> f64 {
    let lon_s = scale(lon, LON_MIN, LON_MAX);
    let lat_s = scale(lat, LAT_MIN, LAT_MAX);
    interleave(lon_s, lat_s) as f64
}

/// Decodes a geohash score back to (lon, lat).
/// Returns the centre of the encoded bucket for better accuracy.
pub fn decode(score: f64) -> (f64, f64) {
    let (lon_s, lat_s) = deinterleave(score as u64);
    let lon = unscale(lon_s, LON_MIN, LON_MAX);
    let lat = unscale(lat_s, LAT_MIN, LAT_MAX);
    (lon, lat)
}

fn scale(v: f64, min: f64, max: f64) -> u64 {
    let s = ((v - min) / (max - min) * STEPS as f64) as u64;
    s.min(STEPS - 1)
}

fn unscale(s: u64, min: f64, max: f64) -> f64 {
    // +0.5 returns the centre of the bucket rather than its lower edge.
    (s as f64 + 0.5) / STEPS as f64 * (max - min) + min
}

/// Interleaves 26 bits of x (longitude) into odd positions and
/// 26 bits of y (latitude) into even positions of a 52-bit result.
fn interleave(x: u64, y: u64) -> u64 {
    let mut h = 0u64;
    for i in 0..26u64 {
        h |= ((x >> i) & 1) << (2 * i + 1);
        h |= ((y >> i) & 1) << (2 * i);
    }
    h
}

fn deinterleave(h: u64) -> (u64, u64) {
    let mut x = 0u64;
    let mut y = 0u64;
    for i in 0..26u64 {
        x |= ((h >> (2 * i + 1)) & 1) << i;
        y |= ((h >> (2 * i)) & 1) << i;
    }
    (x, y)
}

// ---------------------------------------------------------------------------
// Distance
// ---------------------------------------------------------------------------

/// Haversine distance in metres between two (lon, lat) points.
pub fn distance_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    EARTH_RADIUS_M * 2.0 * a.sqrt().asin()
}

// ---------------------------------------------------------------------------
// Unit conversion
// ---------------------------------------------------------------------------

pub fn to_metres(distance: f64, unit: &str) -> f64 {
    match unit.to_lowercase().as_str() {
        "km" => distance * 1_000.0,
        "mi" => distance * 1_609.344,
        "ft" => distance * 0.3048,
        _ => distance, // "m"
    }
}

pub fn from_metres(metres: f64, unit: &str) -> f64 {
    match unit.to_lowercase().as_str() {
        "km" => metres / 1_000.0,
        "mi" => metres / 1_609.344,
        "ft" => metres / 0.3048,
        _ => metres, // "m"
    }
}
