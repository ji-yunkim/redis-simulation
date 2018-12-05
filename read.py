def read():
    while True:
        instr = raw_input("please enter your command: ")
        inputdata=instr.split()
        if inputdata[0].lower() == "SHOW".lower():
            print("SHOW TABLES")
        elif inputdata[0].lower() == "CREATE".lower():
            print("CREATE")
        elif inputdata[0].lower() == "INSERT".lower():
            print("INSERT")
        elif inputdata[0].lower() == "SELECT".lower():
            print("SELECT")
        elif inputdata[0].lower() == "UPDATE".lower():
            print("UPDATE")
        elif inputdata[0].lower() == "DELETE".lower():
            print("DELETE")
        elif inputdata[0].lower() == "QUIT".lower():
            print("Quiting...")
            break;
        else:
            print ("Wrong Command, please try again")


read()