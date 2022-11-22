package connector

import (
	"fmt"
	"strconv"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

func formatObjectID(resourceTypeID string, id int64) string {
	return fmt.Sprintf("%s:%d", resourceTypeID, id)
}

func formatColumnID(tableID int64, columnID int64) string {
	return fmt.Sprintf("%s:%d:%d", columnResourceType.Id, tableID, columnID)
}

func parseObjectID(id string) (int64, error) {
	parts := strings.SplitN(id, ":", 2)
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid object ID %s", id)
	}

	return strconv.ParseInt(parts[1], 10, 64)
}

func parsePageToken(i string, resourceID *v2.ResourceId) (*pagination.Bag, error) {
	b := &pagination.Bag{}
	err := b.Unmarshal(i)
	if err != nil {
		return nil, err
	}

	if b.Current() == nil {
		b.Push(pagination.PageState{
			ResourceTypeID: resourceID.ResourceType,
			ResourceID:     resourceID.Resource,
		})
	}

	return b, nil
}
