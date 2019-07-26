﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using SmallBank.Interfaces;
using Utilities;

namespace SmallBank.Grains
{
    using AmalgamateInput = Tuple<UInt32, UInt32>;
    using WriteCheckInput = Tuple<String, float>;
    using TransactSavingInput = Tuple<String, float>;
    using DepositCheckingInput = Tuple<Tuple<String, UInt32>, float>;
    using BalanceInput = String;
    //Source AccountID, Destination AccountID, Destination Grain ID, Amount
    using TransferInput = Tuple<Tuple<String, UInt32>, Tuple<String, UInt32>, float>;
    //Source AccountID, Amount, List<Tuple<Account Name, Account ID, Grain ID>>
    using MultiTransferInput = Tuple<Tuple<String, UInt32>, float, List<Tuple<String, UInt32>>>;
    using InitAccountInput = Tuple<UInt32, UInt32>;

    class OrleansEventuallyConsistentAccountGroupGrain : Orleans.Grain, IOrleansEventuallyConsistentAccountGroupGrain
    {
        CustomerAccountGroup state = new CustomerAccountGroup();
        public uint numAccountPerGroup = 1;

        private UInt32 MapCustomerIdToGroup(UInt32 accountID)
        {
            return accountID / numAccountPerGroup; //You can can also range/hash partition
        }

        private async Task<FunctionResult> Balance(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult(-1);
            
            try
            {
                var myState = state;
                var custName = (BalanceInput)fin.inputObject;
                if (myState.account.ContainsKey(custName))
                {
                    var id = myState.account[custName];
                    if (!myState.savingAccount.ContainsKey(id) || !myState.checkingAccount.ContainsKey(id))
                    {
                        ret.setException();
                        return ret;
                    }
                    ret.setResult(myState.savingAccount[id] + myState.checkingAccount[id]);
                }
                else
                {
                    ret.setException();
                    return ret;
                }
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        private async Task<FunctionResult> DepositChecking(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = state;
                var inputTuple = (DepositCheckingInput)fin.inputObject;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;
                if (!String.IsNullOrEmpty(custName))
                {
                    id = myState.account[custName];
                }
                if (!myState.checkingAccount.ContainsKey(id))
                {
                    ret.setException();
                    return ret;
                }
                myState.checkingAccount[id] += inputTuple.Item2; //Can also be negative for checking account                
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        public async Task<FunctionResult> TransactSaving(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = state;
                var inputTuple = (TransactSavingInput)fin.inputObject;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    if (!myState.savingAccount.ContainsKey(id))
                    {
                        ret.setException();
                        return ret;
                    }
                    if (myState.savingAccount[id] < inputTuple.Item2)
                    {
                        ret.setException();
                        return ret;
                    }
                    myState.savingAccount[id] -= inputTuple.Item2;
                }
                else
                {
                    ret.setException();
                    return ret;
                }
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        private async Task<FunctionResult> Transfer(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = state;
                var inputTuple = (TransferInput)fin.inputObject;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;

                if (!String.IsNullOrEmpty(custName))
                {
                    id = myState.account[custName];
                }
                if (!myState.checkingAccount.ContainsKey(id) || myState.checkingAccount[id] < inputTuple.Item3)
                {
                    ret.setException();
                    return ret;
                }
                var gID = this.MapCustomerIdToGroup(inputTuple.Item2.Item2);
                FunctionInput funcInput = new FunctionInput(fin, new DepositCheckingInput(inputTuple.Item2, inputTuple.Item3));
                Task<FunctionResult> task;
                if (gID == myState.GroupID)
                {
                    task = DepositChecking(funcInput);
                }
                else
                {
                    var destination = this.GrainFactory.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(Helper.convertUInt32ToGuid(gID));
                    FunctionCall funcCall = new FunctionCall(typeof(CustomerAccountGroupGrain), "DepositChecking", funcInput);
                    task = destination.StartTransaction("DepositChecking", funcInput);
                }

                await task;
                if (task.Result.hasException() == true)
                {
                    ret.setException();
                    return ret;
                }
                ret.mergeWithFunctionResult(task.Result);
                myState.checkingAccount[id] -= inputTuple.Item3;
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        public async Task<FunctionResult> WriteCheck(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = state;
                var inputTuple = (WriteCheckInput)fin.inputObject;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    if (!myState.savingAccount.ContainsKey(id) || !myState.checkingAccount.ContainsKey(id))
                    {
                        ret.setException();
                        return ret;
                    }
                    if (myState.savingAccount[id] + myState.checkingAccount[id] < inputTuple.Item2)
                    {
                        myState.checkingAccount[id] -= (inputTuple.Item2 + 1); //Pay a penalty                        
                    }
                    else
                    {
                        myState.checkingAccount[id] -= inputTuple.Item2;
                    }
                }
                else
                {
                    ret.setException();
                    return ret;
                }
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        public async Task<FunctionResult> MultiTransfer(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = state;
                var inputTuple = (MultiTransferInput)fin.inputObject;
                var custName = inputTuple.Item1.Item1;
                var id = inputTuple.Item1.Item2;
                if (!String.IsNullOrEmpty(custName))
                {
                    id = myState.account[custName];
                }

                if (!myState.checkingAccount.ContainsKey(id) || myState.checkingAccount[id] < inputTuple.Item2 * inputTuple.Item3.Count)
                {
                    ret.setException();
                    return ret;
                }
                else
                {
                    List<Tuple<String, UInt32>> destinations = inputTuple.Item3;
                    List<Task<FunctionResult>> tasks = new List<Task<FunctionResult>>();
                    foreach (var tuple in destinations)
                    {
                        var gID = MapCustomerIdToGroup(tuple.Item2);
                        FunctionInput funcInput = new FunctionInput(fin, new DepositCheckingInput(new Tuple<String, UInt32>(tuple.Item1, tuple.Item2), inputTuple.Item2));
                        if (gID == myState.GroupID)
                        {
                            Task<FunctionResult> localCall = DepositChecking(funcInput);
                            tasks.Add(localCall);
                        }
                        else
                        {
                            var destination = this.GrainFactory.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(Helper.convertUInt32ToGuid(gID));
                            var task = destination.StartTransaction("DepositChecking", funcInput);                            
                            tasks.Add(task);
                        }
                    }
                    await Task.WhenAll(tasks);
                    foreach (Task<FunctionResult> task in tasks)
                    {
                        ret.mergeWithFunctionResult(task.Result);
                    }
                    myState.checkingAccount[id] -= inputTuple.Item2 * inputTuple.Item3.Count;
                }
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        public async Task<FunctionResult> InitBankAccounts(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = state;
                var tuple = (InitAccountInput)fin.inputObject;
                numAccountPerGroup = tuple.Item1;
                myState.GroupID = tuple.Item2;

                uint minAccountID = myState.GroupID * numAccountPerGroup;
                for (uint i = 0; i < numAccountPerGroup; i++)
                {
                    uint accountId = minAccountID + i;
                    myState.account.Add(accountId.ToString(), accountId);
                    myState.savingAccount.Add(accountId, uint.MaxValue);
                    myState.checkingAccount.Add(accountId, uint.MaxValue);
                }
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }


        public async Task<FunctionResult> Amalgamate(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = state;
                var tuple = (AmalgamateInput)fin.inputObject;
                var id = tuple.Item1;
                float balance = 0;

                if (!myState.savingAccount.ContainsKey(id) || !myState.checkingAccount.ContainsKey(id))
                {
                    ret.setException();
                }
                else
                {
                    balance = myState.savingAccount[id] + myState.checkingAccount[id];
                }

                //By invoking with 0 amount and no state mutation, we make the execution deterministic
                var destGrain = this.GrainFactory.GetGrain<IOrleansEventuallyConsistentAccountGroupGrain>(MapCustomerIdToGroup(tuple.Item2));
                var result = await destGrain.StartTransaction("DepositChecking", new FunctionInput(fin, new Tuple<Tuple<String, UInt32>, float>(new Tuple<String, UInt32>(String.Empty, id), balance)));                    
                ret.mergeWithFunctionResult(result);
                if (!ret.hasException())
                {
                    //By ensuring state mutation on no exception, we make it deterministic
                    myState.savingAccount[id] = 0;
                    myState.checkingAccount[id] = 0;
                }
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }
        Task<FunctionResult> IOrleansEventuallyConsistentAccountGroupGrain.StartTransaction(string startFunction, FunctionInput inputs)
        {
            AllTxnTypes fnType;
            if(!Enum.TryParse<AllTxnTypes>(startFunction.Trim(), out fnType)) {
                throw new FormatException($"Unknown function {startFunction}");
            }
            switch (fnType) {
                case AllTxnTypes.Balance:
                    return Balance(inputs);
                case AllTxnTypes.DepositChecking:
                    return DepositChecking(inputs);
                case AllTxnTypes.TransactSaving:
                    return TransactSaving(inputs);
                case AllTxnTypes.WriteCheck:
                    return WriteCheck(inputs);
                case AllTxnTypes.Transfer:
                    return Transfer(inputs);
                case AllTxnTypes.MultiTransfer:
                    return MultiTransfer(inputs);
                case AllTxnTypes.Amalgamate:
                    return Amalgamate(inputs);
                case AllTxnTypes.InitBankAccounts:
                    return InitBankAccounts(inputs);
                default :
                    throw new Exception($"Unknown function {fnType}");
            } 
        }
    }
}