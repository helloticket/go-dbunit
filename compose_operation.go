package dbunit

type ComposeOperation struct {
	op1 Operation
	op2 Operation
}

func (d *ComposeOperation) ExecuteWith(tableName string, fixtureName string) {
	d.op1.ExecuteWith(tableName, fixtureName)
	d.op2.ExecuteWith(tableName, fixtureName)
}

func (d *ComposeOperation) Execute(fixtureName string) {
	d.op1.Execute(fixtureName)
	d.op2.Execute(fixtureName)
}

func (d *ComposeOperation) ExecuteWithFilter(tableName string, fixtureName string, filter Filter) {
	d.op1.ExecuteWithFilter(tableName, fixtureName, filter)
	d.op2.ExecuteWithFilter(tableName, fixtureName, filter)
}
