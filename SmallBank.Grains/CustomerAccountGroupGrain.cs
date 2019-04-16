﻿using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Concurrency.Implementation;
using SmallBank.Interfaces;
using Utilities;

namespace SmallBank.Grains
{
    using DepositSavingInput = Tuple<UInt32, float>;
    using AmalgamateInput = Tuple<UInt32, UInt32>;
    using WriteCheckInput = Tuple<String, float>;
    using TransactSavingInput = Tuple<String, float>;
    using DepositCheckingInput = Tuple<String, float>;
    using BalanceInput = String;

    [Serializable]
    public class CustomerAccountGroup : ICloneable
    {
        public Dictionary<String, UInt32> account;
        public Dictionary<UInt32, float> savingAccount;
        public Dictionary<UInt32, float> checkingAccount;
        object ICloneable.Clone()
        {
            var clonedCustomerAccount = new CustomerAccountGroup();
            clonedCustomerAccount.account = new Dictionary<string, UInt32>(account);
            clonedCustomerAccount.savingAccount = new Dictionary<UInt32, float>(savingAccount);
            clonedCustomerAccount.checkingAccount = new Dictionary<UInt32, float>(checkingAccount);
            return clonedCustomerAccount;
        }
    }

    
    class CustomerAccountGroupGrain : TransactionExecutionGrain<CustomerAccountGroup>, ICustomerAccountGroupGrain
    {
        private UInt32 MapCustomerIdToGroup(UInt32 id)
        {
            return id; //You can can also range/hash partition
        }
        public CustomerAccountGroupGrain() : base("SmallBank.Grains.CustomerAccountGroupGrain")
        {
        }

        async Task<FunctionResult> ICustomerAccountGroupGrain.Amalgamate(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.Read(context);
                var tuple = (AmalgamateInput)fin.inputObject;
                var balance = myState.savingAccount[tuple.Item1] + myState.checkingAccount[tuple.Item1];
                var grain = this.GrainFactory.GetGrain<ICustomerAccountGroupGrain>(MapCustomerIdToGroup(tuple.Item2));
                await grain.Execute(new FunctionCall(typeof(CustomerAccountGroupGrain), "DepositSaving", new FunctionInput(fin, new Tuple<UInt32, float>(tuple.Item1, tuple.Item2))));
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        async Task<FunctionResult> ICustomerAccountGroupGrain.Balance(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult(-1);
            try
            {
                var myState = await state.Read(context);
                var custName = (BalanceInput)fin.inputObject;
                if(myState.account.ContainsKey(custName))
                {
                    var id = myState.account[custName];
                    ret.setResult(myState.savingAccount[id] + myState.checkingAccount[id]);
                } else
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

        async Task<FunctionResult> ICustomerAccountGroupGrain.DepositChecking(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var inputTuple = (DepositCheckingInput)fin.inputObject;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    myState.checkingAccount[id] += inputTuple.Item2; //Can also be negative for checking account
                } else
                {
                    ret.setException();
                    return ret;
                }
            } catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }

        Task<FunctionResult> ICustomerAccountGroupGrain.MultiTransfer(FunctionInput fin)
        {
            throw new NotImplementedException();
        }

        async Task<FunctionResult> ICustomerAccountGroupGrain.TransactSaving(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var inputTuple = (TransactSavingInput)fin.inputObject;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    if(myState.savingAccount[id] < inputTuple.Item2)
                    {
                        ret.setException();
                        return ret;
                    }
                    myState.savingAccount[id] -= inputTuple.Item2;
                } else
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

        Task<FunctionResult> ICustomerAccountGroupGrain.Transfer(FunctionInput fin)
        {
            throw new NotImplementedException();
        }

        async Task<FunctionResult> ICustomerAccountGroupGrain.WriteCheck(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var inputTuple = (WriteCheckInput)fin.inputObject;
                if (myState.account.ContainsKey(inputTuple.Item1))
                {
                    var id = myState.account[inputTuple.Item1];
                    if (myState.savingAccount[id] + myState.checkingAccount[id] < inputTuple.Item2)
                    {
                        myState.checkingAccount[id] -= (inputTuple.Item2 + 1); //Pay a penalty                        
                    } else
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

        async Task<FunctionResult> ICustomerAccountGroupGrain.DepositSaving(FunctionInput fin)
        {
            TransactionContext context = fin.context;
            FunctionResult ret = new FunctionResult();
            try
            {
                var myState = await state.ReadWrite(context);
                var inputTuple = (DepositSavingInput)fin.inputObject;
                myState.savingAccount[inputTuple.Item1] += inputTuple.Item2;                
            }
            catch (Exception)
            {
                ret.setException();
            }
            return ret;
        }
    }
}
