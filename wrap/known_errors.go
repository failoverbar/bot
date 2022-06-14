package wrap

var _ error = NotFoundError{}

type NotFoundError struct{}

func (n NotFoundError) Error() string {
	return "Entity is not found"
}
