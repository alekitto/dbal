mod connection_event;
mod event_dispatcher;
mod schema_alter_table_add_column_event;
mod schema_alter_table_change_column_event;
mod schema_alter_table_event;
mod schema_alter_table_remove_column_event;
mod schema_alter_table_rename_column_event;
mod schema_column_definition_event;
mod schema_create_table_column_event;
mod schema_create_table_event;
mod schema_drop_table_event;
mod schema_index_definition_event;

pub use connection_event::ConnectionEvent;
pub use event_dispatcher::EventDispatcher;
pub use schema_alter_table_add_column_event::SchemaAlterTableAddColumnEvent;
pub use schema_alter_table_change_column_event::SchemaAlterTableChangeColumnEvent;
pub use schema_alter_table_event::SchemaAlterTableEvent;
pub use schema_alter_table_remove_column_event::SchemaAlterTableRemoveColumnEvent;
pub use schema_alter_table_rename_column_event::SchemaAlterTableRenameColumnEvent;
pub use schema_column_definition_event::SchemaColumnDefinitionEvent;
pub use schema_create_table_column_event::SchemaCreateTableColumnEvent;
pub use schema_create_table_event::SchemaCreateTableEvent;
pub use schema_drop_table_event::SchemaDropTableEvent;
pub use schema_index_definition_event::SchemaIndexDefinitionEvent;

use std::any::TypeId;

pub trait Event: Send {
    fn is_async() -> bool
    where
        Self: Sized;
    fn event_type() -> TypeId
    where
        Self: Sized;
}
