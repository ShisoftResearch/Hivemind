use bifrost::membership::client::{ObserverClient, Member};
use bifrost::raft::client::RaftClient;


use std::sync::Arc;
use std::collections::HashMap;

use parking_lot::{RwLock, RwLockReadGuard};

pub enum InitLiveMembersError {
    CannotGetAllMembers
}

pub struct LiveMembers {
    members: Arc<RwLock<HashMap<u64, Member>>>,
    observer: ObserverClient
}

impl LiveMembers {
    pub fn new<'a>(group_name: &'a str, raft_client: &Arc<RaftClient>) -> Result<LiveMembers, InitLiveMembersError> {
        let observer = ObserverClient::new(raft_client);
        let mut members = HashMap::new();
        match observer.all_members(true) {
            Ok(Ok((online_members, _))) =>
                members = online_members
                    .into_iter()
                    .map(|m| (m.id, m))
                    .collect(),
            _ => {
                return Err(InitLiveMembersError::CannotGetAllMembers)
            }
        }
        let members = Arc::new(RwLock::new(members));
        let m1 = members.clone();
        observer.on_group_member_joined(
            move |c| insert(c, &m1), group_name);
        let m2 = members.clone();
        observer.on_group_member_online(
            move |c| insert(c, &m2), group_name);
        let m3 = members.clone();
        observer.on_group_member_left(
            move |c| remove(c, &m3), group_name);
        let m4 = members.clone();
        observer.on_group_member_offline(
            move |c| remove(c, &m4), group_name);
        Ok(LiveMembers {
            members , observer
        })
    }
    pub fn get_by_id(&self, id: u64) -> Option<Member> {
        self.members.read().get(&id).cloned()
    }
    pub fn get_members(&self) -> Vec<Member> {
        self.members.read().values().cloned().collect()
    }
    pub fn get_ids(&self) -> Vec<u64> {
        self.members.read().keys().cloned().collect()
    }
    pub fn members_guarded(&self) -> RwLockReadGuard<HashMap<u64, Member>> {
        self.members.read()
    }
}

fn insert(changes: Result<(Member, u64), ()>, members: &Arc<RwLock<HashMap<u64, Member>>>) {
    let mut owned = members.write();
    if let Ok((ref member, _)) = changes {
        owned.insert(member.id, member.clone());
    }
}

fn remove(changes: Result<(Member, u64), ()>, members: &Arc<RwLock<HashMap<u64, Member>>>) {
    let mut owned = members.write();
    if let Ok((ref member, _)) = changes {
        owned.remove(&member.id);
    }
}
