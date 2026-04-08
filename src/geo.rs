pub const LON_MIN: f64 = -180.0;
pub const LON_MAX: f64 = 180.0;
pub const LAT_MIN: f64 = -85.05112878;
pub const LAT_MAX: f64 = 85.05112878;

const STEPS: u64 = 1 << 26;
const EARTH_RADIUS_M: f64 = 6372797.560856;

pub fn validate(lon: f64, lat: f64) -> bool {
    (LON_MIN..=LON_MAX).contains(&lon) && (LAT_MIN..=LAT_MAX).contains(&lat)
}

pub fn encode(lon: f64, lat: f64) -> f64 {
    let lon_scaled = scale(lon, LON_MIN, LON_MAX);
    let lat_scaled = scale(lat, LAT_MIN, LAT_MAX);
    interleave(lon_scaled, lat_scaled) as f64
}

pub fn decode(score: f64) -> (f64, f64) {
    let (lon_scaled, lat_scaled) = deinterleave(score as u64);
    let lon = unscale(lon_scaled, LON_MIN, LON_MAX);
    let lat = unscale(lat_scaled, LAT_MIN, LAT_MAX);
    (lon, lat)
}

pub fn distance_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let dlat = (lat2 - lat1).to_radians();
    let dlon = (lon2 - lon1).to_radians();
    let a = (dlat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (dlon / 2.0).sin().powi(2);
    EARTH_RADIUS_M * 2.0 * a.sqrt().asin()
}

pub fn to_metres(distance: f64, unit: &str) -> f64 {
    match unit.to_lowercase().as_str() {
        "km" => distance * 1_000.0,
        "mi" => distance * 1_609.344,
        "ft" => distance * 0.3048,
        _ => distance,
    }
}

pub fn from_metres(metres: f64, unit: &str) -> f64 {
    match unit.to_lowercase().as_str() {
        "km" => metres / 1_000.0,
        "mi" => metres / 1_609.344,
        "ft" => metres / 0.3048,
        _ => metres,
    }
}

fn scale(value: f64, min: f64, max: f64) -> u64 {
    let scaled = ((value - min) / (max - min) * STEPS as f64) as u64;
    scaled.min(STEPS - 1)
}

fn unscale(scaled: u64, min: f64, max: f64) -> f64 {
    (scaled as f64 + 0.5) / STEPS as f64 * (max - min) + min
}

fn interleave(x: u64, y: u64) -> u64 {
    let mut hash = 0u64;
    for i in 0..26u64 {
        hash |= ((x >> i) & 1) << (2 * i + 1);
        hash |= ((y >> i) & 1) << (2 * i);
    }
    hash
}

fn deinterleave(hash: u64) -> (u64, u64) {
    let mut x = 0u64;
    let mut y = 0u64;
    for i in 0..26u64 {
        x |= ((hash >> (2 * i + 1)) & 1) << i;
        y |= ((hash >> (2 * i)) & 1) << i;
    }
    (x, y)
}
