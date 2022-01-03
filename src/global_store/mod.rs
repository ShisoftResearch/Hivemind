use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct Job {
  name: String,
  root_task: Uuid,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Task {
  deps: Vec<Uuid>,
  waiting: Vec<Uuid>,
  output: Uuid
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Data {
  node_id: u64,
  loca_id: Uuid
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StoreObject {
  Job(Job),
  Task(Task),
  Data(Data)
}


