namespace Furious

open System
open Definitions
open Interfaces

module Dialect =
    type ANSISqlDialect() =
        interface ISqlDialect with
            member x.Select fields from where = 
                if String.IsNullOrWhiteSpace where then sprintf "select %s from %s" fields from
                else sprintf "select %s from %s where %s" fields from where

            member x.DistinctCount prefix column = sprintf "count(distinct %s.%s)" prefix column
            
            member x.InsertBridge table name1 name2 value1 value2 = sprintf "insert into %s (%s, %s) values (%s, %s)" table name1 name2 value1 value2

            member x.Insert table names values isBatch =
                sprintf "insert into %s (%s) values (%s)%s"
                    table
                    (String.concat ", " names)
                    (String.concat ", " values)
                    (if isBatch then ";" else emptyString)

            member x.BatchSep = ";\r\n"

            member x.Qualify alias name = sprintf "%s.%s" alias name

            member x.NullValue = "null"

            member x.String v = sprintf "'%s'" v

            member x.JoinDirection direction = 
                match direction with
                | Inner -> "inner join"
                | Left -> "left outer join"
                | Right -> "right outer join"

            member x.Join isFirst joinDirection parentName parentAlias name alias primaryKey foreignKey next =
                if isFirst then
                    sprintf "%s as %s %s %s as %s on %s.%s = %s.%s %s"
                        parentName
                        parentAlias
                        ((x :> ISqlDialect).JoinDirection joinDirection)
                        name
                        alias
                        parentAlias
                        foreignKey
                        alias
                        primaryKey
                        next
                else
                    sprintf "%s %s as %s on %s.%s = %s.%s %s"
                        ((x :> ISqlDialect).JoinDirection joinDirection)
                        name
                        alias
                        parentAlias
                        foreignKey
                        alias
                        primaryKey
                        next

            member x.AliasColumn table column alias = sprintf "%s.%s as %s" table column alias

            member x.Value table column = sprintf "%s.%s" table column

            member x.RecBoolean op left right = sprintf "(%s) %s (%s)" left op right
            member x.Boolean (op: string) left right = left + op + right
               

            (*
        sprintf "%s inner join %s as %s on %s.%s = %s.%s %s"
            (if isFirst then sprintf "%s as %s" parentTableName parentTableAlias else emptyString)
            j.table
            j.alias
            parentTableAlias
            j.fkName
            j.alias
            j.pkName
            (List.map (computeFromClauseInternal j.table j.alias context false) jl
             |> String.concat emptyString)
            *)