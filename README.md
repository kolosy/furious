Furious: an F# DSL for DBMS-independent query
========================================

Furious is a compact DSL for expressing data queries without relying on an underlying language. The idea is instead of reinventing a new dsl,
to use the one already present in the F# base libraries - the functions in the Seq module. Consequently, Furious uses quote literals that
encode lambdas operating on the dataset that you want to receive. An example:

    module Samples =
        open Furious.Meta
    
        type person = {
            firstname: string
            lastname: string
            homeAddress: address
            altAddresses: address seq
        }
        and address = {
            street1: string
            zip: string
        }
    
        type personzip = {
            lastname: string
            zip: string
        }
    
        let db = Datastore()
        // simple filtering
        // select firstname, lastname, homeAddressId from people p left outer join addresses a on p.homeAddressId = a.addressId where a.zip = '60614'
        let (neighbor: person seq) = 
            db.Yield <@ fun people-> Seq.filter (fun p -> p.homeAddress.zip = "60614") people @>
    
        // count
        // select count(*) from people p left outer join addresses a on p.homeAddressId = a.addressId where a.zip = '60614'
        let (neighbors: int seq) = 
            db.Compute <@ fun people -> Seq.length <| Seq.filter (fun p -> p.homeAddress.zip = "60614") people @>
    
        // mapping
        // select lastname, zip from from people p left outer join addresses a on p.homeAddressId = a.addressId where a.zip = '60614'
        let (neighborsByZip: personzip seq) = 
            db.Yield <@ fun people -> Seq.collect (fun p -> seq { for address in p.altAddresses -> { lastname = p.lastname; zip = address.zip } }) people @>

This style lets you build type-safe expressions, with the resulting type being the only thing you have to explicitly declare..

Status
------------------------

Furious is beyond raw right now. That said, it will hopefully pick up steam in the coming weeks. Currently, the plan is to build out basic SQL support, 
with CouchDB after that. 