namespace Furious

module Mapper =
    open Interfaces
    open TypeUtils
    open ValueUtils
    
    type DefaultRecordMapper() = 
        interface IRecordMapper with
            member x.MapRecord (tp, (?field: System.Reflection.PropertyInfo)) = 
                match tp with 
                | Sequence -> 
                    match field with
                    | Some f -> f.DeclaringType.Name + (ucFirst f.Name)
                    | None -> failwith "MapRecord called on sequence without field info"
                | _ -> tp.Name

            member x.MapField field = 
                match (match field with 
                                        | :? System.Reflection.PropertyInfo as pi -> pi.PropertyType
                                        | :? System.Reflection.FieldInfo as fi -> fi.FieldType 
                                        | _ -> failwith (sprintf "%A is an unsupported field descriptor" field) ) with
                | Record | Sequence -> field.Name + "Id"
                | _ -> field.Name
            member x.GetPrimaryKeyName tp = Some (tp.Name + "Id") 