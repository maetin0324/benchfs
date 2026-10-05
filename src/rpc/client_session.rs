//! Logical client connections used for dispatch idle policy. The underlying
//! QPs may be retained across finalize/init; sessions close at finalization.
//! A crashed client cannot send close and remains connected until server exit.
use std::collections::HashSet;

pub const RPC_CLIENT_SESSION: u16 = 65534;

#[derive(Default)]
pub struct ClientSessions(HashSet<String>);
impl ClientSessions {
    pub fn count(&self) -> usize {
        self.0.len()
    }
    pub fn update(&mut self, body: &[u8]) -> Result<(), &'static str> {
        let (&op, name) = body.split_first().ok_or("empty session message")?;
        if op > 1 || name.is_empty() || name.len() > 255 {
            return Err("bad session message");
        }
        let name = std::str::from_utf8(name).map_err(|_| "invalid client id")?;
        if op == 1 {
            self.0.insert(name.to_owned());
        } else {
            self.0.remove(name);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn duplicate_connect_disconnect_and_reconnect() {
        let mut sessions = ClientSessions::default();
        assert_eq!(sessions.count(), 0);
        sessions.update(b"\x01client-a").unwrap();
        sessions.update(b"\x01client-a").unwrap();
        sessions.update(b"\x01client-b").unwrap();
        assert_eq!(sessions.count(), 2);
        sessions.update(b"\x00client-a").unwrap();
        sessions.update(b"\x00client-a").unwrap();
        assert_eq!(sessions.count(), 1);
        sessions.update(b"\x00client-b").unwrap();
        assert_eq!(sessions.count(), 0);
        sessions.update(b"\x01client-a").unwrap();
        assert_eq!(sessions.count(), 1);
        assert!(sessions.update(b"\x02client-a").is_err());
        assert!(sessions.update(b"\x01").is_err());
        assert!(sessions.update(b"\x01\xff").is_err());
        assert_eq!(sessions.count(), 1);
    }
}
