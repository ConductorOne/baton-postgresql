package postgres

import (
	"fmt"
	"strings"
)

const NoPrivs = 0

type PrivilegeSet uint64

func (ps PrivilegeSet) String() string {
	switch ps {
	case Select:
		return "r"
	case Insert:
		return "a"
	case Update:
		return "w"
	case Delete:
		return "d"
	case Truncate:
		return "D"
	case References:
		return "x"
	case Trigger:
		return "t"
	case Create:
		return "C"
	case Connect:
		return "c"
	case Temporary:
		return "T"
	case Execute:
		return "X"
	case Usage:
		return "U"
	case Set:
		return "s"
	case AlterSystem:
		return "A"
	}

	return ""
}

func PrivilegeSetFromRune(s rune) PrivilegeSet {
	for key := Insert; key < Terminator; key <<= 1 {
		if string(s) == key.String() {
			return key
		}
	}
	return NoPrivs
}

const (
	Insert PrivilegeSet = 1 << iota
	Select
	Update
	Delete
	Truncate
	References
	Trigger
	Execute
	Usage
	Create
	Temporary
	Connect
	Set
	AlterSystem
	Terminator
)

func (ps PrivilegeSet) Set(priv PrivilegeSet) PrivilegeSet {
	return ps | priv
}

func (ps PrivilegeSet) Has(priv PrivilegeSet) bool {
	return ps&priv != 0
}

type Acl struct {
	privs          PrivilegeSet
	privsWithGrant PrivilegeSet
	grantor        string
	grantee        string
}

func (a *Acl) String() string {
	if a.privs == NoPrivs {
		return ""
	}

	sb := &strings.Builder{}

	grantee := a.grantee
	if grantee == "" {
		grantee = "PUBLIC"
	}

	sb.WriteString(grantee + "=")
	for key := Insert; key < Terminator; key <<= 1 {
		if a.privs.Has(key) {
			sb.WriteString(key.String())
			if a.privsWithGrant.Has(key) {
				sb.WriteString("*")
			}
		}
	}
	sb.WriteString("/" + a.grantor)

	return sb.String()
}

func NewAcl(acl string) (*Acl, error) {
	ret := &Acl{}

	granteeParts := strings.SplitN(acl, "=", 2)
	if len(granteeParts) != 2 {
		return nil, fmt.Errorf("malformed acl: %s", acl)
	}

	ret.grantee = granteeParts[0]
	aclParts := strings.SplitN(granteeParts[1], "/", 2)
	if len(aclParts) != 2 {
		return nil, fmt.Errorf("malformed acl: %s", acl)
	}

	var priv PrivilegeSet
	for _, c := range aclParts[0] {
		switch {
		case c == '*':
			if priv == 0 {
				return nil, fmt.Errorf("malformed acl: %s", acl)
			}
			ret.privsWithGrant = ret.privsWithGrant.Set(priv)
			priv = 0

		case PrivilegeSetFromRune(c) != NoPrivs:
			priv = PrivilegeSetFromRune(c)
			ret.privs = ret.privs.Set(priv)
		}
	}

	ret.grantor = aclParts[1]

	return ret, nil
}
