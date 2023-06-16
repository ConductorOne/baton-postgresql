package postgres

import (
	"fmt"
	"strings"
)

type ACLResource interface {
	GetOwnerID() int64
	GetACLs() []string
	AllPrivileges() PrivilegeSet
	DefaultPrivileges() PrivilegeSet
}

const EmptyPrivilegeSet = PrivilegeSet(0)

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
	default:
		return ""
	}
}

func (ps PrivilegeSet) Name() string {
	switch ps {
	case Select:
		return "SELECT"
	case Insert:
		return "INSERT"
	case Update:
		return "UPDATE"
	case Delete:
		return "DELETE"
	case Truncate:
		return "TRUNCATE"
	case References:
		return "REFERENCES"
	case Trigger:
		return "TRIGGER"
	case Create:
		return "CREATE"
	case Connect:
		return "CONNECT"
	case Temporary:
		return "TEMPORARY"
	case Execute:
		return "EXECUTE"
	case Usage:
		return "USAGE"
	case Set:
		return "SET"
	case AlterSystem:
		return "ALTER SYSTEM"
	default:
		return ""
	}
}

func PrivilegeSetFromRune(s rune) PrivilegeSet {
	for key := Insert; key < Terminator; key <<= 1 {
		if string(s) == key.String() {
			return key
		}
	}
	return EmptyPrivilegeSet
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

func (a PrivilegeSet) Range(f func(p PrivilegeSet) (bool, error)) error {
	for key := Insert; key < Terminator; key <<= 1 {
		ok, err := f(key)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
	}

	return nil
}

type ACL struct {
	privs          PrivilegeSet
	privsWithGrant PrivilegeSet
	grantor        string
	grantee        string
}

func (a *ACL) Privileges() PrivilegeSet {
	return a.privs
}

func (a *ACL) GrantPrivileges() PrivilegeSet {
	return a.privsWithGrant
}

func (a *ACL) Check(p PrivilegeSet) (bool, bool) {
	return a.privs&p == p, a.privsWithGrant&p == p
}

func (a *ACL) Grantee() string {
	return a.grantee
}

func (a *ACL) String() string {
	if a.privs == EmptyPrivilegeSet {
		return ""
	}

	sb := &strings.Builder{}

	grantee := a.grantee
	if grantee == "" {
		grantee = "PUBLIC"
	}

	_, _ = sb.WriteString(grantee + "=")
	for key := Insert; key < Terminator; key <<= 1 {
		if a.privs.Has(key) {
			_, _ = sb.WriteString(key.String())
			if a.privsWithGrant.Has(key) {
				_, _ = sb.WriteString("*")
			}
		}
	}
	_, _ = sb.WriteString("/" + a.grantor)

	return sb.String()
}

func NewACL(acl string) (*ACL, error) {
	ret := &ACL{}

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

		case PrivilegeSetFromRune(c) != EmptyPrivilegeSet:
			priv = PrivilegeSetFromRune(c)
			ret.privs = ret.privs.Set(priv)
		}
	}

	ret.grantor = aclParts[1]

	return ret, nil
}

func NewACLFromPrivilegeSets(privs PrivilegeSet, privsWithGrant PrivilegeSet) *ACL {
	return &ACL{
		privs:          privs,
		privsWithGrant: privsWithGrant,
	}
}
