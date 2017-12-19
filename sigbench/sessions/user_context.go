package sessions

type UserContext struct {
	UserId  string
	Phase   string
	Params  map[string]string
	Control chan string
}
