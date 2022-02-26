package kvraft

type StateMachine struct {
	//mu sync.Mutex
	KV map[string]string
}

func MakeStateMachine() *StateMachine {
	return &StateMachine{
		KV: make(map[string]string),
	}
}
func (vm *StateMachine) Execute(op Op)  Response{
	res := Response{}
	if op.Operator == "Get"{
		res.Value,res.Err = vm.Get(op.Key)
	}else if op.Operator == "Append"{
		res.Value,res.Err = vm.Append(op.Key,op.Value)
	}else{
		res.Value,res.Err = vm.Put(op.Key,op.Value)
	}
	return res
}
func (vm *StateMachine) Get(Key string) (string,Err) {
	if value,ok := vm.KV[Key];ok{
		return value,OK
	}
	return "",ErrNoKey
}

func (vm *StateMachine) Put(Key string,Value string) (string,Err)  {
	vm.KV[Key] = Value
	return "",OK
}

func (vm *StateMachine) Append(Key string,Value string) (string,Err) {
	vm.KV[Key] += Value
	return "",OK
}