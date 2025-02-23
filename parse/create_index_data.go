package parse

type CreateIndexData struct {
	indexName string
	fieldName string
	tableName string
}

func NewCreateIndexData(indexName string, fieldName string, tableName string) *CreateIndexData {
	return &CreateIndexData{
		indexName,
		fieldName,
		tableName,
	}
}

func (c *CreateIndexData) IndexName() string {
	return c.indexName
}

func (c *CreateIndexData) TableName() string {
	return c.tableName
}

func (c *CreateIndexData) FieldName() string {
	return c.fieldName
}
