package controller

import "github.com/google/uuid"

type robotID uuid.UUID

func robotNameToID(name string) robotID {
	id := uuid.NewSHA1(uuid.NameSpaceDNS, []byte(name))
	return robotID(id)
}

func (id robotID) String() string {
	return uuid.UUID(id).String()
}
