package service

import (
	"context"

	"agrisync/internal/model"
)

// ListPolicies exposes the residency policy that replication and delivery
// consult. It is read-only over the API: policy changes are an out-of-band
// governance action in this demo deployment.
func (s *Service) ListPolicies(ctx context.Context) ([]model.Policy, error) {
	rows, err := s.db.Control().Query(ctx, `
		select region_code, category, outbound_allowed, rationale, updated_at
		from residency_policies order by region_code,
		  case category when 'raw_trajectory' then 1 when 'operation_summary' then 2
		                when 'device_alarm' then 3 else 4 end`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Policy{}
	for rows.Next() {
		var p model.Policy
		if err := rows.Scan(&p.RegionCode, &p.Category, &p.OutboundAllowed, &p.Rationale, &p.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
