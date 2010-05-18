namespace Furious

module Interfaces =
    /// entity responsible for mapping names defined in the type system into external names
    /// of both entities, and entity attributes
    type IRecordMapper = 
        /// Retrieves the external entity name of the given record type
        abstract member MapRecord : System.Type -> System.String
        /// Retrieves the external attribute name of the given attribute
        abstract member MapField : System.Reflection.MemberInfo -> System.String
        /// Retrieves the primary key for the given entity. Returns None if the underlying
        /// persistance system does not define the concept of primary keys
        abstract member GetPrimaryKeyName : System.Type -> System.String option