// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package noop

import (
	"fmt"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
)

// NoopType describes a function whose Process step is a no-op.
const NoopType = "no-op"

// NewNoop creates a new noop operation
func NewNoop() (parser.Params, error) {
	return baseOp{}, nil
}

// baseOp stores required properties for the baseOp
type baseOp struct{}

func (o baseOp) OpType() string {
	return NoopType
}

func (o baseOp) String() string {
	return fmt.Sprintf("type: %s", o.OpType())
}

func (o baseOp) Node(
	controller *transform.Controller,
	_ transform.Options,
) transform.OpNode {
	return &baseNode{
		op:         o,
		controller: controller,
	}
}

type baseNode struct {
	op         baseOp
	controller *transform.Controller
}

func (n *baseNode) Process(
	queryCtx *models.QueryContext,
	_ parser.NodeID,
	b block.Block,
) error {
	return n.controller.Process(queryCtx, b)
}
