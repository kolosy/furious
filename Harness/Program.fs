
open Furious.Meta

type person = {
    personId: string
    firstname: string
    lastname: string
    homeAddress: address
    workAddress: address
    altAddresses: address seq
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

let conn = new System.Data.SQLite.SQLiteConnection("Data Source=:memory:;Version=3;New=True")
conn.Open()
let cmd = conn.CreateCommand()
printfn "Creating tables..."
cmd.CommandText <- "create table person (personId guid primary key not null, firstname varchar, lastname varchar, homeAddressId varchar, workAddressId varchar)"
cmd.ExecuteNonQuery() |> ignore
cmd.CommandText <- "create table address (addressId guid primary key not null, street1 varchar, zip varchar)"
cmd.ExecuteNonQuery() |> ignore
cmd.CommandText <- "create table personAltAddresses (addressId guid not null, personId guid not null)"
cmd.ExecuteNonQuery() |> ignore

let db = Datastore(fun () -> upcast conn)

let address1 = { addressId = System.Guid.NewGuid().ToString(); street1 = "1232 something"; zip = "60614" }
let address2 = { addressId = System.Guid.NewGuid().ToString(); street1 = "456 something"; zip = "60614" }
let address3 = { addressId = System.Guid.NewGuid().ToString(); street1 = "2344 something"; zip = "60614" }
let address4 = { addressId = System.Guid.NewGuid().ToString(); street1 = "987 something"; zip = "60614" }

//let newPerson = { personId = System.Guid.NewGuid().ToString(); firstname = "alex"; lastname = "pedenko"; homeAddress = address1; workAddress = address2; altAddresses = [address3; address4] }
let newPerson = { personId = System.Guid.NewGuid().ToString(); firstname = "alex"; lastname = "pedenko"; homeAddress = address1; workAddress = address2; altAddresses = [ address3; address4 ] }

//printf "\r\n Case 0"
//db.Save newPerson |> ignore

printfn "\r\n\r\nCase 1\r\n" |> ignore
let (neighbor: person seq) = db.Yield <@ Seq.filter (fun p -> p.homeAddress.zip = "60614") @>
printfn "<\r\n" |> ignore
Seq.iter (printfn "%A") neighbor
printfn "\r\n>" |> ignore

printfn "\r\n\r\nCase 2\r\n" |> ignore
let (neighbors: int seq) = db.Compute <@ fun people -> Seq.length <| Seq.filter (fun p -> p.homeAddress.zip = "60614") people @>
printfn "<\r\n" |> ignore
Seq.iter (printfn "%A") neighbors
printfn "\r\n>" |> ignore

printfn "\r\n\r\nCase 3\r\n" |> ignore
let (neighbor2: person seq) = db.Yield <@ Seq.filter (fun p -> (p.homeAddress.zip = "60614") && (p.workAddress.zip = "60069") && ((p.firstname = "alex") || (p.lastname = "pedenko"))) @>
printfn "<\r\n" |> ignore
Seq.iter (printfn "%A") neighbor2
printfn "\r\n>" |> ignore
    
printfn "\r\n\r\nCase 4\r\n" |> ignore
let (neighbors2: int seq) = db.Compute <@ fun people -> Seq.length <| Seq.filter (fun p -> (p.homeAddress.zip = "60614") && (p.workAddress.zip = "60069") && ((p.firstname = "alex") || (p.lastname = "pedenko"))) people @>
printfn "<\r\n" |> ignore
Seq.iter (printfn "%A") neighbors2
printfn "\r\n>" |> ignore

printf "Done. Press any key to continue." |> ignore
System.Console.ReadLine() |> ignore

