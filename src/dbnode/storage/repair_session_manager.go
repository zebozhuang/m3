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

package storage

import (
	"sync"

	// TODO(r): change to m3db/m3/src/x
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
)

type repairSessionManager struct {
	sync.RWMutex

	db       database
	sessions map[string]*repairSession
	slice    []databaseRepairSession
}

func newRepairSessionManager(
	db database,
) *repairSessionManager {
	return &repairSessionManager{
		db:       db,
		sessions: make(map[string]*repairSession),
	}
}

func (m *repairSessionManager) StartSession(id ident.ID) error {
	m.Lock()
	result := m.newSessionWithLock(id)
	m.Unlock()

	if result.prev != nil {
		// Close the previous session with this ID after lock
		// released to avoid blocking others
		err := result.prev.Close()
		if err != nil {
			return err
		}
	}

	return nil
}

type setSessionResult struct {
	prev *repairSession
}

func (m *repairSessionManager) newSessionWithLock(
	id ident.ID,
) setSessionResult {
	var result setSessionResult

	if existing, ok := m.sessions[id.String()]; ok {
		result.prev = existing
		delete(m.sessions, id.String())
	}

	// Start a new session
	session := newRepairSession(id)
	m.sessions[id.String()] = session

	// Reset the slice of sessions, need a new slice
	// since callers might have references to old views
	slice := make([]databaseRepairSession, 0, len(m.sessions))
	for _, session := range m.sessions {
		slice = append(slice, session)
	}

	return result
}

func (m *repairSessionManager) Sessions() []databaseRepairSession {
	m.RLock()
	v := m.slice
	m.RUnlock()
	return v
}

func (m *repairSessionManager) Close() error {
	m.Lock()
	defer m.Unlock()

	var multiErr xerrors.MultiError
	for id, session := range m.sessions {
		multiErr.Add(session.Close())
		delete(m.sessions, id)
	}

	return multiErr.FinalError()
}
