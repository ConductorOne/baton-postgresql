package postgres

import (
	"reflect"
	"testing"
)

func TestPrivilegeSet_Set(t *testing.T) {
	type args struct {
		priv PrivilegeSet
	}
	tests := []struct {
		name string
		ps   PrivilegeSet
		args args
		want PrivilegeSet
	}{
		{
			"select, insert, update",
			Select,
			args{Insert | Update},
			Select | Insert | Update,
		},
		{
			"empty",
			NoPrivs,
			args{NoPrivs},
			NoPrivs,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ps.Set(tt.args.priv); got != tt.want {
				t.Errorf("Set() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPrivilegeSet_Has(t *testing.T) {
	type args struct {
		priv PrivilegeSet
	}
	tests := []struct {
		name string
		ps   PrivilegeSet
		args args
		want bool
	}{
		{
			"empty doesn't have select",
			NoPrivs,
			args{Select},
			false,
		},
		{
			"select has select",
			Select,
			args{Select},
			true,
		},
		{
			"select and insert has select",
			Select | Insert,
			args{Select},
			true,
		},
		{
			"select and insert has insert",
			Select | Insert,
			args{Insert},
			true,
		},
		{
			"select and insert doesn't have update",
			Select | Insert,
			args{Insert},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ps.Has(tt.args.priv); got != tt.want {
				t.Errorf("Has() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAcl_String(t *testing.T) {
	type fields struct {
		privs          PrivilegeSet
		privsWithGrant PrivilegeSet
		grantor        string
		grantee        string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			"empty",
			fields{
				NoPrivs,
				NoPrivs,
				"bar",
				"foo",
			},
			"",
		},
		{
			"select",
			fields{
				Select,
				NoPrivs,
				"bar",
				"foo",
			},
			"foo=r/bar",
		},
		{
			"select, insert with grant",
			fields{
				Select | Insert,
				Insert,
				"bar",
				"foo",
			},
			"foo=a*r/bar",
		},
		{
			"select, insert with grant, update, delete, create, connect with grant",
			fields{
				Select | Insert | Update | Delete | Create | Connect,
				Insert | Connect,
				"bar",
				"foo",
			},
			"foo=a*rwdCc*/bar",
		},
		{
			"select, insert with grant, update, delete, create, connect with grant out of order",
			fields{
				Delete | Insert | Connect | Select | Update | Create,
				Connect | Insert,
				"bar",
				"foo",
			},
			"foo=a*rwdCc*/bar",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Acl{
				privs:          tt.fields.privs,
				privsWithGrant: tt.fields.privsWithGrant,
				grantor:        tt.fields.grantor,
				grantee:        tt.fields.grantee,
			}
			if got := a.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewAcl(t *testing.T) {
	type args struct {
		acl string
	}
	tests := []struct {
		name    string
		args    args
		want    *Acl
		wantErr bool
	}{
		{
			"empty",
			args{""},
			nil,
			true,
		},
		{
			"foo is granted select by bar",
			args{"foo=r/bar"},
			&Acl{
				grantee: "foo",
				grantor: "bar",
				privs:   Select,
			},
			false,
		},
		{
			"foo is granted select, insert with grant, create with grant, delete by bar",
			args{"foo=ra*C*d/bar"},
			&Acl{
				grantee:        "foo",
				grantor:        "bar",
				privs:          Select | Insert | Create | Delete,
				privsWithGrant: Insert | Create,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewAcl(tt.args.acl)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewAcl() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewAcl() got = %v, want %v", got, tt.want)
			}
		})
	}
}
