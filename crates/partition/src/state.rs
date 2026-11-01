use std::collections::BTreeMap;
use std::io;
use trading_protocol::{Command, CommandKind, CommandResult, Status};

#[derive(Default)]
pub struct State {
    accounts: BTreeMap<u64, u64>,
    results: BTreeMap<u128, CommandResult>,
}

impl State {
    pub fn execute(&mut self, sequence: u64, command: Command) -> CommandResult {
        if let Some(result) = self.results.get(&command.request_id) {
            return *result;
        }
        let status = self.apply(command);
        let result = CommandResult {
            request_id: command.request_id,
            sequence,
            status,
        };
        self.results.insert(command.request_id, result);
        result
    }

    fn apply(&mut self, command: Command) -> Status {
        match command.kind {
            CommandKind::OpenAccount => match self.accounts.entry(command.primary_account) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(0);
                    Status::Success
                }
                std::collections::btree_map::Entry::Occupied(_) => Status::AccountExists,
            },
            CommandKind::Deposit => self.deposit(command.primary_account, command.amount),
            CommandKind::Withdraw => self.withdraw(command.primary_account, command.amount),
            CommandKind::Transfer => self.transfer(command),
        }
    }

    fn deposit(&mut self, account: u64, amount: u64) -> Status {
        if amount == 0 {
            return Status::InvalidAmount;
        }
        let Some(balance) = self.accounts.get_mut(&account) else {
            return Status::AccountNotFound;
        };
        let Some(updated) = balance.checked_add(amount) else {
            return Status::BalanceOverflow;
        };
        *balance = updated;
        Status::Success
    }

    fn withdraw(&mut self, account: u64, amount: u64) -> Status {
        if amount == 0 {
            return Status::InvalidAmount;
        }
        let Some(balance) = self.accounts.get_mut(&account) else {
            return Status::AccountNotFound;
        };
        let Some(updated) = balance.checked_sub(amount) else {
            return Status::InsufficientFunds;
        };
        *balance = updated;
        Status::Success
    }

    fn transfer(&mut self, command: Command) -> Status {
        let from = command.primary_account;
        let to = command.secondary_account;
        let amount = command.amount;
        if amount == 0 {
            return Status::InvalidAmount;
        }
        if from == to {
            return Status::InvalidTransfer;
        }
        let Some(from_balance) = self.accounts.get(&from).copied() else {
            return Status::AccountNotFound;
        };
        let Some(to_balance) = self.accounts.get(&to).copied() else {
            return Status::AccountNotFound;
        };
        let Some(updated_from) = from_balance.checked_sub(amount) else {
            return Status::InsufficientFunds;
        };
        let Some(updated_to) = to_balance.checked_add(amount) else {
            return Status::BalanceOverflow;
        };
        self.accounts.insert(from, updated_from);
        self.accounts.insert(to, updated_to);
        Status::Success
    }

    pub fn balance(&self, account: u64) -> Option<u64> {
        self.accounts.get(&account).copied()
    }

    pub fn total_balance(&self) -> u128 {
        self.accounts.values().map(|balance| *balance as u128).sum()
    }

    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }

    pub fn result_count(&self) -> usize {
        self.results.len()
    }

    pub fn encode_snapshot(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(16 + self.accounts.len() * 16 + self.results.len() * 32);
        out.extend_from_slice(&(self.accounts.len() as u64).to_le_bytes());
        out.extend_from_slice(&(self.results.len() as u64).to_le_bytes());
        for (account, balance) in &self.accounts {
            out.extend_from_slice(&account.to_le_bytes());
            out.extend_from_slice(&balance.to_le_bytes());
        }
        for (request_id, result) in &self.results {
            out.extend_from_slice(&request_id.to_le_bytes());
            out.extend_from_slice(&result.sequence.to_le_bytes());
            out.push(result.status.to_u8());
            out.extend_from_slice(&[0; 7]);
        }
        out
    }

    pub fn decode_snapshot(bytes: &[u8]) -> io::Result<Self> {
        let mut reader = Reader::new(bytes);
        let account_count = reader.read_u64()? as usize;
        let result_count = reader.read_u64()? as usize;
        if account_count > reader.remaining() / 16 {
            return Err(invalid_data("invalid snapshot account count"));
        }
        let mut accounts = BTreeMap::new();
        for _ in 0..account_count {
            accounts.insert(reader.read_u64()?, reader.read_u64()?);
        }
        if result_count > reader.remaining() / 32 {
            return Err(invalid_data("invalid snapshot result count"));
        }
        let mut results = BTreeMap::new();
        for _ in 0..result_count {
            let request_id = reader.read_u128()?;
            let sequence = reader.read_u64()?;
            let status = Status::from_u8(reader.read_u8()?)?;
            reader.skip(7)?;
            results.insert(
                request_id,
                CommandResult {
                    request_id,
                    sequence,
                    status,
                },
            );
        }
        if reader.remaining() != 0 {
            return Err(invalid_data("trailing snapshot state bytes"));
        }
        Ok(Self { accounts, results })
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        if self.remaining() < len {
            return Err(invalid_data("truncated snapshot state"));
        }
        let start = self.offset;
        self.offset += len;
        Ok(&self.bytes[start..start + len])
    }

    fn read_u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn read_u128(&mut self) -> io::Result<u128> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }

    fn skip(&mut self, len: usize) -> io::Result<()> {
        self.take(len).map(|_| ())
    }
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_is_atomic_and_conserves_balance() {
        let mut state = State::default();
        assert!(state.execute(1, Command::open_account(1, 10)).is_success());
        assert!(state.execute(2, Command::open_account(2, 20)).is_success());
        state.execute(3, Command::deposit(3, 10, 100));
        assert_eq!(
            state.execute(4, Command::transfer(4, 10, 20, 30)).status,
            Status::Success
        );
        assert_eq!(state.balance(10), Some(70));
        assert_eq!(state.balance(20), Some(30));
        assert_eq!(state.total_balance(), 100);
        assert_eq!(
            state.execute(5, Command::transfer(5, 10, 20, 100)).status,
            Status::InsufficientFunds
        );
        assert_eq!(state.balance(10), Some(70));
        assert_eq!(state.balance(20), Some(30));
    }

    #[test]
    fn duplicate_request_returns_original_result_without_replaying_effects() {
        let mut state = State::default();
        state.execute(1, Command::open_account(1, 10));
        state.execute(2, Command::deposit(2, 10, 100));
        let first = state.execute(3, Command::withdraw(3, 10, 40));
        let duplicate = state.execute(4, Command::withdraw(3, 10, 40));
        assert_eq!(first, duplicate);
        assert_eq!(duplicate.sequence, 3);
        assert_eq!(state.balance(10), Some(60));
        assert_eq!(state.result_count(), 3);
    }

    #[test]
    fn snapshot_state_round_trip_preserves_idempotency() {
        let mut state = State::default();
        state.execute(1, Command::open_account(1, 10));
        state.execute(2, Command::deposit(2, 10, 100));
        let bytes = state.encode_snapshot();
        let mut restored = State::decode_snapshot(&bytes).unwrap();
        let duplicate = restored.execute(3, Command::deposit(2, 10, 100));
        assert_eq!(duplicate.sequence, 2);
        assert_eq!(restored.balance(10), Some(100));
    }
}
