use trading_protocol::{Command, CommandKind, Route, RouteError, RouteVersion, ShardId};

/// Immutable, versioned mapping from fixed routing slots to logical shards.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RouteTable {
    version: RouteVersion,
    slots: Vec<ShardId>,
}

impl RouteTable {
    pub fn new(version: RouteVersion, slots: Vec<ShardId>) -> Result<Self, RouteError> {
        if slots.is_empty() {
            return Err(RouteError::EmptyTable);
        }
        Ok(Self { version, slots })
    }

    pub fn uniform(version: RouteVersion, shard_count: ShardId) -> Result<Self, RouteError> {
        if shard_count == 0 {
            return Err(RouteError::EmptyTable);
        }
        Self::new(version, (0..shard_count).collect())
    }

    pub fn version(&self) -> RouteVersion {
        self.version
    }

    pub fn slots(&self) -> &[ShardId] {
        &self.slots
    }

    pub fn shard_ids(&self) -> impl Iterator<Item = ShardId> + '_ {
        self.slots.iter().copied()
    }

    pub fn route_account(&self, account: u64) -> Route {
        let slot = (account % self.slots.len() as u64) as usize;
        Route {
            version: self.version,
            shard_id: self.slots[slot],
        }
    }

    pub fn route(&self, command: &Command) -> Result<Route, RouteError> {
        let primary = self.route_account(command.primary_account);
        if command.kind == CommandKind::Transfer {
            let secondary = self.route_account(command.secondary_account);
            if primary.shard_id != secondary.shard_id {
                return Err(RouteError::CrossShard { primary, secondary });
            }
        }
        Ok(primary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uniform_table_routes_accounts_to_every_shard() {
        let table = RouteTable::uniform(7, 4).unwrap();
        assert_eq!(table.version(), 7);
        for shard in 0..4 {
            assert!(table.shard_ids().any(|candidate| candidate == shard));
        }
        assert_eq!(table.route_account(6).shard_id, 2);
    }

    #[test]
    fn transfer_must_stay_on_one_logical_shard() {
        let table = RouteTable::uniform(1, 2).unwrap();
        assert_eq!(
            table
                .route(&Command::transfer(1, 2, 4, 10))
                .unwrap()
                .shard_id,
            0
        );
        assert!(matches!(
            table.route(&Command::transfer(2, 2, 3, 10)),
            Err(RouteError::CrossShard { .. })
        ));
    }

    #[test]
    fn empty_table_is_rejected() {
        assert_eq!(RouteTable::new(1, Vec::new()), Err(RouteError::EmptyTable));
        assert_eq!(RouteTable::uniform(1, 0), Err(RouteError::EmptyTable));
    }
}
