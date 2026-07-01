use std::fs::OpenOptions;
use std::io::Write;

pub struct WakeLock {
    name: String,
}

impl WakeLock {
    pub fn new(name: &str) -> Option<Self> {
        match OpenOptions::new().write(true).open("/sys/power/wake_lock") {
            Ok(mut file) => {
                if let Err(_e) = file.write_all(name.as_bytes()) {
                    None
                } else {
                    Some(Self {
                        name: name.to_string(),
                    })
                }
            }
            Err(_e) => {
                None
            }
        }
    }
}

impl Drop for WakeLock {
    fn drop(&mut self) {
        if let Ok(mut file) = OpenOptions::new().write(true).open("/sys/power/wake_unlock") {
            let _ = file.write_all(self.name.as_bytes());
        }
    }
}

