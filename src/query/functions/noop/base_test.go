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
	"testing"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor/transform"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type sinkNode struct {
	b block.Block
}

func (n *sinkNode) Process(
	_ *models.QueryContext,
	_ parser.NodeID,
	block block.Block,
) error {
	n.b = block
	return nil
}

func TestNoop(t *testing.T) {
	op, err := NewNoop()
	assert.NoError(t, err)

	assert.Equal(t, "no-op", op.OpType())
	assert.Equal(t, "type: no-op", op.String())

	base, ok := op.(baseOp)
	require.True(t, ok)

	c := &transform.Controller{ID: parser.NodeID(2)}
	sink := &sinkNode{}
	c.AddTransform(sink)

	node := base.Node(c, transform.Options{})
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	b := block.NewMockBlock(ctrl)
	queryCtx := &models.QueryContext{}
	err = node.Process(queryCtx, parser.NodeID(1), b)
	require.NoError(t, err)
	require.Equal(t, b, sink.b)
}
