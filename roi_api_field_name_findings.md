# ROI CRM API — Field Name Findings (2026-03-19)

## Summary

The ROI `/donors/` search endpoint returns field names that differ from what the
`ROICRMConnector` tests and internal code assumed. This caused silent failures in
the `action_network_webhooks` project when parsing search results.

## Actual Field Names (from live API)

The following were confirmed by calling `search_donors(**{"roi-id": "..."})` against
the live ROI API and inspecting the raw response:

| What we assumed | Actual field name | Notes |
|---|---|---|
| `id` | `roi_family_id` | Individual donor ID (ROI Family ID) |
| `first_name` | `name_first` | Given name |
| `last_name` | `name_last` | Family name |

## Full Field List (search result item)

A `search_donors()` result item contains these fields:

```
links                        # HAL-style rel links (self, emails, donations, etc.)
roi_id                       # Household identifier (shared by all household members)
roi_family_id                # Individual donor identifier (unique per member)
origination_vendor           # e.g. "ACTION_NETWORK"
last_change_date             # ISO timestamp
last_change_user             # Username string
name_first                   # Given name
name_last                    # Family name
name_full                    # Full name string
salutation                   # Preferred salutation
address_line                 # Formatted address line for this member
household_address_line       # Combined household address line
is_deceased                  # String "true"/"false"
family_member_type_code      # "HEAD_OF_HOUSEHOLD" or "SECONDARY_CONTACT"
family_member_type_name      # Human-readable equivalent
partner_roi_family_id        # Partner's individual ID (if household has 2 members)
partner_name_first           # Partner's given name
partner_name_last            # Partner's family name
partner_is_deceased          # String "true"/"false"
partner_family_member_type_code
partner_family_member_type_name
do_not_contact               # String "true"/"false"
account_status               # e.g. "ACTIVE"
account_added_date           # ISO timestamp
modified_date                # ISO timestamp
```

## Key Data Model Notes

- **ROI ID** = household identifier. All members of a household share the same `roi_id`.
- **ROI Family ID** = individual identifier. Each household member has a unique `roi_family_id`.
- For the Head of Household: `roi_family_id == roi_id` (self-referential).
- `search_donors(**{"roi-id": roi_id})` returns all members of the household (typically 1–2).

## `get_primary_address()` Behavior

`get_primary_address(roi_family_id)` returns 404 for some donors — this appears to be
expected when the donor has no address on record. The caller should handle this with
try/except rather than treating 404 as an error condition.

## Impact on `_roi_raw_to_record` Helper

The `action_network_webhooks` project had a private `_roi_raw_to_record()` helper that
was using `raw.get("id")`, `raw.get("first_name")`, and `raw.get("last_name")` — all of
which return `None` against the real API. This caused all household member matching to
fail silently (every `donor_id` was `None`). Fixed in commit `be28965`.

The same issue affects any code that post-processes `search_donors()` results using
the assumed field names.

## Recommendation for ccef-connections

1. **Update connector docstrings** for `search_donors()` to document the actual response
   field names, including the full field list above.
2. **Update or replace test fixtures** — current tests mock search results as
   `{"id": 1, "last_name": "Smith"}`, which does not reflect the real API schema.
   Consider adding integration-style fixtures or at minimum correcting the mock keys.
3. **Consider a `RoiDonor` dataclass** in the connector that normalizes search results
   to predictable Python attribute names, so callers don't have to know the raw API keys.
