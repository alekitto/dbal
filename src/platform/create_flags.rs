use bitflags::bitflags;

bitflags! {
    pub struct CreateFlags: u32 {
        const CREATE_INDEXES      = 0x1;
        const CREATE_FOREIGN_KEYS = 0x2;
    }
}
