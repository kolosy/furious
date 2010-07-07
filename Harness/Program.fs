// Learn more about F# at http://fsharp.net

open Furious.Meta

type person = {
    personId: string
    firstname: string
    lastname: string
    homeAddress: address
    workAddress: address
    //altAddresses: address seq
}
and address = {
    addressId: string
    street1: string
    zip: string
}

type personzip = {
    lastname: string
    zip: string
}

let conn = new MySql.Data.MySqlClient.MySqlConnection("Server=localhost;Database=test;Uid=test;Pwd=test;") :> System.Data.Common.DbConnection
conn.Open()

let db = Datastore(fun () -> conn)

let address1 = { addressId = System.Guid.NewGuid().ToString(); street1 = "1232 something"; zip = "60614" }
let address2 = { addressId = System.Guid.NewGuid().ToString(); street1 = "456 something"; zip = "60614" }
let address3 = { addressId = System.Guid.NewGuid().ToString(); street1 = "2344 something"; zip = "60614" }
let address4 = { addressId = System.Guid.NewGuid().ToString(); street1 = "987 something"; zip = "60614" }

//let newPerson = { personId = System.Guid.NewGuid().ToString(); firstname = "alex"; lastname = "pedenko"; homeAddress = address1; workAddress = address2; altAddresses = [address3; address4] }
let newPerson = { personId = System.Guid.NewGuid().ToString(); firstname = "alex"; lastname = "pedenko"; homeAddress = address1; workAddress = address2; }

//printf "\r\n Case 4"
//db.Save newPerson |> ignore

printfn "\r\nCase 1" |> ignore
let (neighbor: person seq) = db.Yield <@ fun people-> Seq.filter (fun p -> p.homeAddress.zip = "60614") people @>

printfn "\r\nCase 2" |> ignore
let (neighbors: int seq) = db.Compute <@ fun people -> Seq.length <| Seq.filter (fun p -> p.homeAddress.zip = "60614") people @>

printfn "\r\nCase 3" |> ignore
let (neighbor2: person seq) = db.Yield <@ Seq.filter (fun p -> (p.homeAddress.zip = "60614") && (p.workAddress.zip = "60069") && ((p.firstname = "alex") || (p.lastname = "pedenko"))) @>
    
printf "Done. Press any key to continue." |> ignore
System.Console.ReadLine() |> ignore

