pub enum TxState {
    Inactive,
    Active(Vec<Vec<String>>),
}

impl TxState {
    pub fn new() -> Self {
        TxState::Inactive
    }

    pub fn is_active(&self) -> bool {
        matches!(self, TxState::Active(_))
    }

    pub fn begin(&mut self) {
        *self = TxState::Active(Vec::new());
    }

    pub fn reset(&mut self) {
        *self = TxState::Inactive;
    }

    pub fn enqueue(&mut self, args: Vec<String>) {
        if let TxState::Active(queue) = self {
            queue.push(args);
        }
    }

    pub fn take_queue(&mut self) -> Vec<Vec<String>> {
        if let TxState::Active(queue) = self {
            let cmds = queue.clone();
            self.reset();
            cmds
        } else {
            vec![]
        }
    }
}
