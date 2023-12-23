package app

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

/*
	简单二次封装使用以下两个包
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
*/

func NewControl(name string) IControl {
	return &control{
		command: &cobra.Command{
			Use:   name,
			Short: name,
		},
	}
}

func NewControlWithRoute(name string, routeFunc ControlRouteFunc) IControl {
	return &control{
		command: &cobra.Command{
			Use:   name,
			Short: name,
		},
		routeFunc: routeFunc,
	}
}

type control struct {
	command   *cobra.Command
	routeFunc ControlRouteFunc
}

func (c *control) AddRouteFunc(routeFunc ControlRouteFunc) {
	c.routeFunc = routeFunc
}

func (c *control) makeOption(opts ...string) (opt ControlOption) {
	opt = ControlOption{}
	length := len(opts)
	if length > 0 {
		opt.Short = opts[0]
	}

	if length > 1 {
		opt.Long = opts[1]
	}

	if length > 2 {
		opt.Example = opts[2]
	}

	return
}

func (c *control) addControl(cmd *cobra.Command) *control {
	c.command.AddCommand(cmd)
	return &control{
		command: cmd,
	}
}

func (c *control) Group(name string, opts ...string) IControlGroup {
	opt := c.makeOption(opts...)

	cmd := &cobra.Command{
		Use:     name,
		Short:   opt.Short,
		Long:    opt.Long,
		Example: opt.Example,
	}

	return c.addControl(cmd)
}

func (c *control) Handler(name string, handler IControlHandler, opts ...string) {
	var param *pflag.FlagSet
	opt := c.makeOption(opts...)
	cmd := &cobra.Command{
		Use:     name,
		Short:   opt.Short,
		Long:    opt.Long,
		Example: opt.Example,
		Run: func(cmd *cobra.Command, args []string) {
			handler.Handle(cmd, param, args)
		},
	}

	param = cmd.PersistentFlags()
	handler.Params(param)
	c.addControl(cmd)
}

func (c *control) Execute() error {
	c.routeFunc(c)
	return c.command.Execute()
}

type ControlRouteFunc func(route IControlGroup)

type ControlOption struct {
	Long    string
	Short   string
	Example string
}

type IControlHandler interface {
	Params(set *pflag.FlagSet)
	Handle(cmd *cobra.Command, param *pflag.FlagSet, args []string)
}

type IControlGroup interface {
	Group(name string, opts ...string) IControlGroup
	Handler(name string, handler IControlHandler, opts ...string)
}

type IControl interface {
	IControlGroup
	AddRouteFunc(routeFunc ControlRouteFunc)
	Execute() error
}
