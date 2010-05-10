namespace Furious

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
    let (neighbor: person seq) = db.Yield <@ fun people-> Seq.filter (fun p -> p.homeAddress.zip = "60614") people @>

    // count
    // select count(*) from people p left outer join addresses a on p.homeAddressId = a.addressId where a.zip = '60614'
    let (neighbors: int seq) = db.Compute <@ fun people -> Seq.length <| Seq.filter (fun p -> p.homeAddress.zip = "60614") people @>

    // mapping
    // select lastname, zip from from people p left outer join addresses a on p.homeAddressId = a.addressId where a.zip = '60614'
    let (neighborsByZip: personzip seq) = db.Yield <@ fun people -> Seq.collect (fun p -> seq { for address in p.altAddresses -> { lastname = p.lastname; zip = address.zip } }) people @>
