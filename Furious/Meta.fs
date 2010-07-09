namespace Furious

module Meta =
    open Microsoft.FSharp.Quotations
    open Microsoft.FSharp.Quotations.ExprShape
    open Microsoft.FSharp.Quotations.Patterns
    open Microsoft.FSharp.Quotations.DerivedPatterns
    open Microsoft.FSharp.Reflection

    open Join
    open Expression
    open Interfaces
    open RecordMapping

    type Datastore(conn: unit -> System.Data.Common.DbConnection, ?keyMapper:IRecordMapper) =
        let defaultMapper = 
            { new IRecordMapper with
                member x.MapRecord tp = tp.Name
                member x.MapField field = 
                    if FSharpType.IsRecord (match field with 
                                            | :? System.Reflection.PropertyInfo as pi -> pi.PropertyType
                                            | :? System.Reflection.FieldInfo as fi -> fi.FieldType 
                                            | _ -> failwith (sprintf "%A is an unsupported field descriptor" field) ) then
                        field.Name + "Id"
                    else
                        field.Name
                member x.GetPrimaryKeyName tp = Some (tp.Name + "Id") }

        let prefix = "t1"

        member private x.Mapper with get() = match keyMapper with | Some m -> m | None -> defaultMapper
        
        member private x.RunSql sql =
            let command = conn().CreateCommand() 
            command.CommandText <- sql
            command.ExecuteReader()

        member x.Yield (expr: Expr<(seq<'a>->seq<'b>)>) = 
            let joins = computeJoins typeof<'b> x.Mapper prefix
            let e,collation = traverse prefix joins x.Mapper None expr
            let select = 
                match collation with 
                | Some e -> failwith (sprintf "unknown collation function %s" e)
                | None ->
                    computeSelectClause typeof<'a> prefix joins x.Mapper
            
            let from = computeFromClause typeof<'a> prefix x.Mapper joins

            let sql = sprintf "select %s from %s %s" select from (if System.String.IsNullOrWhiteSpace e then "" else "where " + e)
            printfn "\r\n running sql \r\n%s" sql
            seq {
                use reader = x.RunSql sql

                while reader.Read() do
                    for r in (readRecord typeof<'a> prefix (x.Mapper) reader None) -> r :?> 'b
            }

        member x.Compute (expr: Expr<(seq<'a>->'b)>) = 
            let joins = computeJoins typeof<'a> x.Mapper prefix
            let e,collation = traverse prefix joins x.Mapper None expr
            let select = 
                match collation with 
                | Some "length" ->
                    sprintf "count(*)"
                | Some e -> failwith (sprintf "unknown collation function %s" e)
                | None ->
                    computeSelectClause typeof<'a> prefix joins x.Mapper

            let from = computeFromClause typeof<'a> prefix x.Mapper joins
            let sql = sprintf "select %s from %s %s" select from (if System.String.IsNullOrWhiteSpace e then "" else "where " + e)
            printfn "\r\n running sql \r\n%s" sql
            seq {
                use reader = x.RunSql sql

                while reader.Read() do
                    for r in (readRecord typeof<'b> prefix (x.Mapper) reader None) -> r :?> 'b
            }

        member x.Save record =
            let cmd = conn().CreateCommand()
            cmd.CommandText <- writeRecord x.Mapper false record
            cmd.CommandType <- System.Data.CommandType.Text
            cmd.ExecuteNonQuery()