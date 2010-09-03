namespace Furious

open System
open Definitions

module Interfaces =
    /// entity responsible for mapping names defined in the type system into external names
    /// of both entities, and entity attributes
    type IRecordMapper = 
        /// Retrieves the external entity name of the given record type
        abstract member MapRecord : tp:Type * ?field:Reflection.PropertyInfo -> String
        /// Retrieves the external attribute name of the given attribute
        abstract member MapField : Reflection.MemberInfo -> String
        /// Retrieves the primary key for the given entity. Returns None if the underlying
        /// persistance system does not define the concept of primary keys
        abstract member GetPrimaryKeyName : Type -> String option

    type ISqlDialect =
        abstract member Select : string -> string -> string -> string
        abstract member DistinctCount : string -> string -> string
        abstract member InsertBridge : string -> string -> string -> string -> string -> string
        abstract member Insert : string -> string seq -> string seq -> bool -> string
        abstract member BatchSep : string 
        abstract member Qualify : string -> string -> string
        abstract member NullValue : string
        abstract member String : string -> string
        abstract member JoinDirection : direction -> string
        abstract member Join : bool -> direction -> string -> string -> string -> string -> string -> string -> string -> string
        abstract member AliasColumn : string -> string -> string -> string
        abstract member Value : string -> string -> string
        abstract member RecBoolean : string -> string -> string -> string
        abstract member Boolean : string -> string -> string -> string

    /// aggregate of information necessary to interact with the environment
    type context = {
        mapper: IRecordMapper
        dialect: ISqlDialect
    }