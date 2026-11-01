//! Fixed-width binary commands shared by the WAL, state machine, and clients.

use std::fmt;
use std::io;

pub const PROTOCOL_VERSION: u8 = 1;
pub const COMMAND_LEN: usize = 44;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandKind {
    OpenAccount,
    Deposit,
    Withdraw,
    Transfer,
}

impl CommandKind {
    fn to_u8(self) -> u8 {
        match self {
            Self::OpenAccount => 1,
            Self::Deposit => 2,
            Self::Withdraw => 3,
            Self::Transfer => 4,
        }
    }

    fn from_u8(value: u8) -> io::Result<Self> {
        match value {
            1 => Ok(Self::OpenAccount),
            2 => Ok(Self::Deposit),
            3 => Ok(Self::Withdraw),
            4 => Ok(Self::Transfer),
            _ => Err(invalid_data(format!("unknown command kind: {value}"))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Command {
    pub request_id: u128,
    pub kind: CommandKind,
    pub primary_account: u64,
    pub secondary_account: u64,
    pub amount: u64,
}

impl Command {
    pub fn open_account(request_id: u128, account_id: u64) -> Self {
        Self {
            request_id,
            kind: CommandKind::OpenAccount,
            primary_account: account_id,
            secondary_account: 0,
            amount: 0,
        }
    }

    pub fn deposit(request_id: u128, account_id: u64, amount: u64) -> Self {
        Self {
            request_id,
            kind: CommandKind::Deposit,
            primary_account: account_id,
            secondary_account: 0,
            amount,
        }
    }

    pub fn withdraw(request_id: u128, account_id: u64, amount: u64) -> Self {
        Self {
            request_id,
            kind: CommandKind::Withdraw,
            primary_account: account_id,
            secondary_account: 0,
            amount,
        }
    }

    pub fn transfer(request_id: u128, from: u64, to: u64, amount: u64) -> Self {
        Self {
            request_id,
            kind: CommandKind::Transfer,
            primary_account: from,
            secondary_account: to,
            amount,
        }
    }

    /// Layout: version/kind/reserved (4), request ID (16), accounts (16), amount (8).
    pub fn encode(self) -> [u8; COMMAND_LEN] {
        let mut out = [0_u8; COMMAND_LEN];
        out[0] = PROTOCOL_VERSION;
        out[1] = self.kind.to_u8();
        out[4..20].copy_from_slice(&self.request_id.to_le_bytes());
        out[20..28].copy_from_slice(&self.primary_account.to_le_bytes());
        out[28..36].copy_from_slice(&self.secondary_account.to_le_bytes());
        out[36..44].copy_from_slice(&self.amount.to_le_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != COMMAND_LEN {
            return Err(invalid_data(format!(
                "command must be {COMMAND_LEN} bytes, got {}",
                bytes.len()
            )));
        }
        if bytes[0] != PROTOCOL_VERSION {
            return Err(invalid_data(format!(
                "unsupported protocol version: {}",
                bytes[0]
            )));
        }
        Ok(Self {
            request_id: u128::from_le_bytes(bytes[4..20].try_into().unwrap()),
            kind: CommandKind::from_u8(bytes[1])?,
            primary_account: u64::from_le_bytes(bytes[20..28].try_into().unwrap()),
            secondary_account: u64::from_le_bytes(bytes[28..36].try_into().unwrap()),
            amount: u64::from_le_bytes(bytes[36..44].try_into().unwrap()),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Status {
    Success,
    AccountExists,
    AccountNotFound,
    InsufficientFunds,
    InvalidAmount,
    InvalidTransfer,
    BalanceOverflow,
}

impl Status {
    pub fn to_u8(self) -> u8 {
        match self {
            Self::Success => 0,
            Self::AccountExists => 1,
            Self::AccountNotFound => 2,
            Self::InsufficientFunds => 3,
            Self::InvalidAmount => 4,
            Self::InvalidTransfer => 5,
            Self::BalanceOverflow => 6,
        }
    }

    pub fn from_u8(value: u8) -> io::Result<Self> {
        match value {
            0 => Ok(Self::Success),
            1 => Ok(Self::AccountExists),
            2 => Ok(Self::AccountNotFound),
            3 => Ok(Self::InsufficientFunds),
            4 => Ok(Self::InvalidAmount),
            5 => Ok(Self::InvalidTransfer),
            6 => Ok(Self::BalanceOverflow),
            _ => Err(invalid_data(format!("unknown status: {value}"))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandResult {
    pub request_id: u128,
    pub sequence: u64,
    pub status: Status,
}

impl CommandResult {
    pub fn is_success(self) -> bool {
        self.status == Status::Success
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmitError {
    Overloaded,
    Stopped,
    Storage(String),
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overloaded => write!(f, "partition queue is full"),
            Self::Stopped => write!(f, "partition executor has stopped"),
            Self::Storage(message) => write!(f, "storage failure: {message}"),
        }
    }
}

impl std::error::Error for SubmitError {}

fn invalid_data(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_round_trip_uses_fixed_width_encoding() {
        let command = Command::transfer(u128::MAX - 7, 11, 22, 33);
        let encoded = command.encode();
        assert_eq!(encoded.len(), COMMAND_LEN);
        assert_eq!(Command::decode(&encoded).unwrap(), command);
    }

    #[test]
    fn decoder_rejects_invalid_version_and_length() {
        assert!(Command::decode(&[0; 3]).is_err());
        let mut encoded = Command::deposit(1, 2, 3).encode();
        encoded[0] = 99;
        assert!(Command::decode(&encoded).is_err());
    }
}
