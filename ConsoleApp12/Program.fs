open System
open System.IO
open System.Diagnostics
open System.Diagnostics
open System.Collections.Generic
open System.Threading.Tasks
open System.Xml.XPath
open System.Collections.Concurrent
open System.Xml.Linq
open System.Globalization


type Store = class
    val Code : string
    val Location : string

    new (code, location) =
        { Code = code; Location = location; }

end

type Date ( year: int, week: int) =
    
    member x.Year = year
    member x.Week = week

   
    override x.Equals (b) =
        match b with
        | :? Date as d -> ( year , week ) = (d.Year , d.Week)
        | _ -> false
    override x.ToString () =
        ("0000" + x.Year.ToString()).Substring(("0000" + x.Year.ToString()).Length - 4 , 4) + ("00" + x.Week.ToString()).Substring(("00" + x.Week.ToString()).Length - 2 , 2)
    override x.GetHashCode() =
        hash ( year , week)
        


type Order = class
    val Store : Store
    val Date : Date
    val SupplierType: string
    val SupplierName: string
    val Cost: decimal

    new (store, date, supplierType, supplierName, cost) =
        { Store = store; Date = date; SupplierType = supplierType; SupplierName = supplierName; Cost = cost; }

end
type Index = class
    val Store : Store
    val Date : Date   

    new (store, date) =
        { Store = store; Date = date;}

end
type OrderDetail = class   
    val SupplierType: string
    val SupplierName: string
    val Cost: decimal

    new ( supplierType, supplierName, cost) =
        { SupplierType = supplierType; SupplierName = supplierName; Cost = cost; }
   

end

module Console =

    open System

    let log =
        let lockObj = obj()
        fun color s ->
            lock lockObj (fun _ ->
                Console.ForegroundColor <- color
                printf "%s" s
                Console.ResetColor())

    let complete = log ConsoleColor.Magenta
    let ok = log ConsoleColor.Green
    let info = log ConsoleColor.Cyan
    let warn = log ConsoleColor.Yellow
    let error = log ConsoleColor.Red

//change here the defaut path accordingly
let DefaultPath: string = "D:\\task-based\\ExampleLoadData\\ExampleLoadData\\bin\\Debug\\netcoreapp2.1\\StoreData\\"
let storeCodesFile: string = "StoreCodes.csv"
let folderPath: string = "StoreData\\"

let stores = new Dictionary<string, Store>()
//let dates = new Queue<Date>()
let orders = new Queue<Order>()
let orders2 = new ConcurrentBag<Order>()
let orders3 = new ConcurrentDictionary<Index, OrderDetail[]>()
let dates = new ConcurrentDictionary<Date, Date>()
let supplierTypes = new ConcurrentDictionary<string, string>()
let supplierNames = new ConcurrentDictionary<string, string>()

let datesHS = new HashSet<Date>()
let snHS = new HashSet<string> ()
let stHS = new HashSet<string> ()
let storesHS = new HashSet<string> ()
let yearsHS = new HashSet<string> ()

let weekInYearString = new Dictionary<string , HashSet<string>> ()



let ToFileNameWithOUTExt (s: string) : string  =
    (((s.Split '\\').[(s.Split '\\').Length-1]).Split '\u002E').[0]

let ToFileNameWithExt (s: string) : string  =
    ((s.Split '\\').[(s.Split '\\').Length-1])

let ToDate (s: string) : Date  =
    new Date (Convert.ToInt32((s.Split '_').[2]), Convert.ToInt32((s.Split '_').[1]))

let ToOrderDetail (s : string) : OrderDetail =
    new OrderDetail (( s.Split ',').[1], ( s.Split ',').[0], Convert.ToDecimal(( s.Split ',').[2]) )

let ToOrdetailArray (s : string[]) : OrderDetail  [] =
    s |> Array.map(fun x -> ToOrderDetail(x))

 

let FirstOrDefaultS (x: string, y: string) : string =
    if (x = "" ) then
        y    
    else
        x
let FirstOrDefaultI (x: int, y: int) : int =
    if (x = 0 ) then
        y    
    else
        x
  

let ToIndex (s : string) : Index =
    new Index (
    stores.[ string ((s.Split '_').[0])],
    new Date(
    Convert.ToInt32((s.Split '_').[2]), 
    Convert.ToInt32((s.Split '_').[1])
    )
    )

let QueryArray (oa : OrderDetail [] , supplierType: string, supplierName: string) : decimal =
    query {
        for ele in oa do
            where (ele.SupplierName = FirstOrDefaultS(supplierName, ele.SupplierName))
            where (ele.SupplierType = FirstOrDefaultS(supplierType,ele.SupplierType))
            sumBy ele.Cost
    }

let QueryTotals (storeId : string, year : int , week : int ,  supplierType: string, supplierName: string) : decimal =
    
    let reduceOrders =
        query {
            for o in orders3 do
                where (o.Key.Store.Code = FirstOrDefaultS(storeId, o.Key.Store.Code))
                where (o.Key.Date.Year = FirstOrDefaultI(year, o.Key.Date.Year))
                where (o.Key.Date.Week = FirstOrDefaultI(week, o.Key.Date.Week))
                select o.Value
        }

    let suma2 = new ConcurrentBag<decimal>()
    Parallel.ForEach(reduceOrders, (fun file ->
        suma2.Add(
            QueryArray(file, supplierType, supplierName)
        )
        
         
        )
    ) 
    
    suma2 |> Seq.cast<decimal> |> Seq.sum

let addSupplierTypes (ord :  OrderDetail []) =
    for elem in ord do
        supplierTypes.TryAdd(elem.SupplierType, elem.SupplierType)
 
let addSupplierNames (ord :  OrderDetail []) =
    for elem in ord do
        supplierNames.TryAdd(elem.SupplierName, elem.SupplierName)
 

let GetDictionaries ( )  =
    let suma2 = new ConcurrentBag<decimal>()
    Parallel.ForEach(orders3.Values, (fun file ->
        addSupplierTypes(file)
        addSupplierNames(file)
        ) 
    ) |> ignore 
    suma2 |> Seq.cast<decimal> |> Seq.sum


type DataF  () =
    member x.LoadData (path : string) =          
        //printfn "%s" path
        use stream = new StreamReader(@"" + path + "\\"  + "StoreCodes.csv")
        let mutable valid = true
        let stopWatch = new Stopwatch()
        stopWatch.Start()
        while (valid) do
            let line = stream.ReadLine()
            if (line = null) then
                valid <- false
            else
            let lineSplit = line.Split ','
            let store: Store = new Store(lineSplit.[0], lineSplit.[1])
            //printfn "%s %s" store.Code store.Location
            stores.Add(lineSplit.[0], store)
        let fileNames: string[] = Directory.GetFiles(@"" + path + "\\" + folderPath)

        let doParallelForEach =
            Parallel.ForEach(fileNames, (fun file ->
                let date: Date = ToDate (ToFileNameWithOUTExt(file))
                dates.TryAdd(date, date) |> ignore
                orders3.TryAdd(
                ToIndex(ToFileNameWithOUTExt(file)),
                ToOrdetailArray(File.ReadAllLines(path + "\\" + folderPath + "\\" + ToFileNameWithExt(file))))                 
                |> ignore

                )
            )

        stopWatch.Stop()
        Console.complete <| String.Format ("\n\t\tThe data was loaded in {0} miliseconds\n\n", stopWatch.Elapsed.TotalMilliseconds)


let rec GetStringFromHash (HS : HashSet<string>, prompt: string) : string =
    let rn = (new System.Random()).Next(0, (HS.Count-1))
    //printf "%i -- %i" rn (HS.Count)
    let elem  = Array.ofSeq HS
    printf "\t\t%s (ex: %s) :> " prompt (elem.[rn].ToString())
    
    let input  = Console.ReadLine()
    if (HS.Contains(input)) then input
    else
        Console.error <| String.Format("\t\tERROR: \"{0}\" not found, try again...\n ", input)
        GetStringFromHash(HS, prompt)
  

let PressKey () = 
    Console.warn <| "\t\tPress any key to continue..."
    Console.ReadKey() |> ignore
let ShowBigTotal() =
    let res = String.Format( "\n\t\tThe total cost of all orders available in the supplied data is: {0}\n", 
    ((QueryTotals("",0,0,"","")).ToString("C"))   )
    Console.complete <| res
    PressKey() 
let C2_ShowTotalByStore() =
    
    let st = GetStringFromHash(storesHS, "Enter Store code")
    let res = String.Format( "\n\t\tThe total of sales for store \"{0}\"({1}) is {2}\n",  
    (stores.[st].Location), st, ((QueryTotals(st,0,0,"","")).ToString("C")) )
    Console.complete <| res
    PressKey()
let C3_ShowTotalByWeek() =
    
    let year = GetStringFromHash(yearsHS, "Enter year")
    let week = GetStringFromHash(weekInYearString.[year], "Enter week")
    let res = String.Format( "\n\t\tThe total of sales in week \"{0}\" of {1} is: {2}\n",
    week, year, ((QueryTotals("",Convert.ToInt32(year),Convert.ToInt32(week),"","")).ToString("C")) )
    Console.complete <| res
    PressKey()
let C4_ShowTotalPerStoreByWeek() =
    let st = GetStringFromHash(storesHS, "Enter Store code")
    let year = GetStringFromHash(yearsHS, "Enter year")
    let week = GetStringFromHash(weekInYearString.[year], "Enter week")
    let res = String.Format( "\n\t\tThe total cost of all orders for store \"{0}\"({1}), in week {2} of {3}: {4}\n" ,
    (stores.[st].Location), st, week, year, ((QueryTotals(st,Convert.ToInt32(year),Convert.ToInt32(week),"","")).ToString("C"))  )
    Console.complete <| res
    PressKey()
let C5_ShowTotalBySupplierName() =
    let sn = GetStringFromHash(snHS, "Enter supplier name")
    let res = String.Format ( "\n\t\tThe total of sales for supplier with name \"{0}\"  is {1}\n" ,
    (sn) , ((QueryTotals("",0,0,"",sn)).ToString("C"))  )
    Console.complete <| res
    PressKey()
let C6_ShowTotalBySupplierType() =
    let sty = GetStringFromHash(stHS, "Enter supplier type")
    let res =  String.Format (  "\n\t\tThe total of sales for supplier with name \"{0}\"  is {1}\n" ,
    (sty) , ((QueryTotals("",0,0,sty,"")).ToString("C"))  )
    Console.complete <| res
    PressKey()
let C7_ShowTotalBySupplierTypePerWeek() =
    let sty = GetStringFromHash(stHS, "Enter Supplier Type")
    let year = GetStringFromHash(yearsHS, "Enter year")
    let week = GetStringFromHash(weekInYearString.[year], "Enter week")
    let res = String.Format(  "\n\t\tThe total of sales for supplier type \"{0}\" in week {1} of {2}  is {3}\n" ,
    sty, week, year, ((QueryTotals("",Convert.ToInt32(year),Convert.ToInt32(week),sty,"")).ToString("C"))  )
    Console.complete <| res
    PressKey()
let C8_ShowTotalBySupplierTypePerStore() =
    let st = GetStringFromHash(storesHS, "Enter Store code")
    let sty = GetStringFromHash(stHS, "Enter supplier type")
    let res = String.Format( "\n\t\tThe total of sales for store \"{0}\"({1}) with supplier type \"{2}\"  is {3}\n" 
    , (stores.[st].Location) ,st, (sty) , ((QueryTotals(st,0,0,sty,"")).ToString("C"))  )
    Console.complete <| res
    PressKey()
let C9_ShowTotalBySupplierTypePerStorePerWeek() =
    let st = GetStringFromHash(storesHS, "Enter Store code")
    let sty = GetStringFromHash(stHS, "Enter supplier type")
    let year = GetStringFromHash(yearsHS, "Enter year")
    let week = GetStringFromHash(weekInYearString.[year], "Enter week")
    let res =  String.Format(  "\n\t\tThe total of sales for store \"{0}\"({1}) with supplier type \"{2}\" in week {3} of {4} is {5}\n",
        (stores.[st].Location) ,  st , (sty) ,  week , year ,  
        ((QueryTotals(st,Convert.ToInt32(year),Convert.ToInt32(week),sty,"")).ToString("C"))  )
    Console.complete <| res
    PressKey()
let PrintStore(entry : string ) =
    Console.warn <| "\t\t" + entry 
    printf " -- "
    Console.info <| (stores.[entry].Location ) + "\n"
let C10_ListStore() = 
    printf "\t\tEnter a store name (ex. %s) or store code (ex. %s ) \n\t\tto filter the list or just the ENTER key to list all the stores\n" ((Array.ofSeq storesHS).[0]) (stores.[((Array.ofSeq storesHS).[0])].Location)
    printf "\t\tEnter store name or code:> "    
    let input  = Console.ReadLine()
    printfn ""
    if (input = "") then 
        for entry in storesHS do
            PrintStore(entry)
    else
        for entry in storesHS do
        if (entry.ToUpper().Contains(input.ToUpper()) || (stores.[entry].Location.ToUpper().Contains(input.ToUpper())) ) then 
           PrintStore(entry)

    PressKey()
let C11_ListSupplierNames() = 
    printf "\t\tEnter a supplier name (ex. %s)\n\t\tto filter the list or just the ENTER key to list all the supplier names\n" ((Array.ofSeq snHS).[0])
    printf "\t\tEnter supplier name or code:> "    
    let input  = Console.ReadLine()
    printfn ""
    if (input = "") then 
        for entry in snHS do
            Console.info <| "\t\t" + entry + "\n"
    else
        for entry in snHS do
        if (entry.ToUpper().Contains(input.ToUpper()) ) then 
            Console.info <| "\t\t" + entry + "\n"

    PressKey()
let C12_ListSupplierTypes() = 
    printf "\t\tEnter a supplier name (ex. %s)\n\t\tto filter the list or just the ENTER key to list all the supplier names\n"  ((Array.ofSeq stHS).[0])
    printf "\t\tEnter supplier name or code:> "    
    let input  = Console.ReadLine()
    printfn ""
    if (input = "") then 
        for entry in stHS do
            Console.info <| "\t\t" + entry + "\n"
    else
        for entry in stHS do
        if (entry.ToUpper().Contains(input.ToUpper()) ) then 
            Console.info <| "\t\t" + entry + "\n"

    PressKey()


    



let CheckInput (input : string) =
    if (input = "1") then
        (ShowBigTotal())
    elif ((input = "2")) then
        (C2_ShowTotalByStore())
    elif ((input = "3")) then
        C3_ShowTotalByWeek()
    elif ((input = "4")) then
        C4_ShowTotalPerStoreByWeek()
    elif ((input = "5")) then
        C5_ShowTotalBySupplierName()
    elif ((input = "6")) then
        C6_ShowTotalBySupplierType()
    elif ((input = "7")) then
        C7_ShowTotalBySupplierTypePerWeek()
    elif ((input = "8")) then
        C8_ShowTotalBySupplierTypePerStore()
    elif ((input = "9")) then
        C9_ShowTotalBySupplierTypePerStorePerWeek()
    elif ((input = "10")) then
        C10_ListStore()
    elif ((input = "11")) then
         C11_ListSupplierNames()
    elif ((input = "12")) then
         C12_ListSupplierTypes()
    else
        Console.error <| "\t\tOPTION NOT RECOGNIZED\n"
        PressKey()
 
    
    
let PrintMainMenu () =

    let mutable quit = false
    while quit <> true do
        Console.info <| "\n\n\tMAIN MENU\n"
        Console.ok "\tSelect an option\n"
        Console.info <| "\t1. Show the total cost of all orders available in the supplied data\n"
        Console.info <| "\t2. Show The total cost of all orders for a single store\n"
        Console.info <| "\t3. Show the total cost of orders in a week for all stores\n"
        Console.info <| "\t4. Show the total cost of orders in a week for a single store\n"
        Console.info <| "\t5. Show the total cost of all orders to a supplier name\n"
        Console.info <| "\t6. Show the total cost of all orders to a supplier type\n"
        Console.info <| "\t7. Show the cost of orders in a week for a supplier type\n"
        Console.info <| "\t8. Show the cost of orders for a supplier type for a store\n"
        Console.info <| "\t9. Show the cost of orders in a week for a supplier type for a store\n"
        Console.warn <| "\t10. List all Stores\n"
        Console.warn <| "\t11. List all Supplier Names\n"
        Console.warn <| "\t12. List all Supplier Types\n"
        Console.complete <| "\t14. Exit\n"
        printf "\tOption: > "
        let input = Console.ReadLine()

        match input with
        | "-q" -> quit <- true
        | "-quit" -> quit <- true
        |"14" -> quit <- true
        | _ -> CheckInput(input)



let getDatesHashSet () =
    for entry in dates  do
        datesHS.Add(entry.Key) |>  ignore

let getSNHasSet () =
    for entry in supplierNames  do
        snHS.Add(entry.Key) |>  ignore
 
let getSTHasSet () =
    for entry in supplierTypes  do
        stHS.Add(entry.Key) |>  ignore
let getStoreHasSet () =
    for entry in stores  do
        storesHS.Add(entry.Value.Code) |>  ignore
let getYearseHasSet () =
    for entry in dates  do
        yearsHS.Add( string (entry.Key.Year) ) |>  ignore
let getYearWeeksHasSetString () =
    for entry in datesHS  do
        if not (weekInYearString.ContainsKey( string (entry.Year) )) then
            weekInYearString.Add( string (entry.Year)  , new HashSet<string>())
    for entry2 in datesHS  do        
            weekInYearString.[ string (entry2.Year)  ].Add(  string  (entry2.Week) )
            |> ignore
        
    //for entry3 in weekInYearString do
    //    for entry4 in entry3.Value do
    //        printfn "%s -- %s" entry3.Key entry4
    
    
let Welcome() =
    Console.info <| "\t\tWELCOME TO THE SYSTEM\n\n"

let  ChooseFolder () : string =
    printf "\t\tEnter The Path where the Data is (Enter for Default Path):> "
    let input  = Console.ReadLine()
    if (input = "" ) then DefaultPath
    else  input.Trim '\"'




let Do_func () =
    
    Welcome()
    let fp = ChooseFolder ()
    if ((File.Exists(fp  + "\\" + storeCodesFile))
        && (Directory.Exists(fp + "\\" + folderPath))) then
        DataF().LoadData(fp)
    
        GetDictionaries()
    
        getDatesHashSet()
        getSNHasSet ()
        getSTHasSet ()
        getStoreHasSet()
        getYearseHasSet()
        getYearWeeksHasSetString()

        PrintMainMenu()    
    else
    Console.error <| "\n\n\n\t\t\t\tIncorrect Path... Try to run the program again...\n\t\t"
    Console.complete <| "\n\n\n\t\t\t\tProgram Finished...\n\t\t"
    PressKey()


Do_func()
|> ignore
