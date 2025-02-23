package parse

type CreateViewData struct {
	viewName  string
	queryData *QueryData
}

func NewCreateViewData(viewName string, queryData *QueryData) *CreateViewData {
	return &CreateViewData{viewName, queryData}
}

func (c *CreateViewData) ViewName() string {
	return c.viewName
}

func (c *CreateViewData) QueryData() *QueryData {
	return c.queryData
}
