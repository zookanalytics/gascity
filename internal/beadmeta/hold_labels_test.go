package beadmeta

import "testing"

// TestActiveHoldValue pins what a hold label is. Any nonempty value is an
// active hold, because the value's vocabulary belongs to the city or pack that
// sets it; hold:cleared is the inactive state bd leaves when a hold is
// released; and a hold: label with no value, or a label that only contains the
// word, is not a hold at all.
func TestActiveHoldValue(t *testing.T) {
	cases := []struct {
		label     string
		wantValue string
		wantOK    bool
	}{
		{"hold:mayor", "mayor", true},
		{"hold:external", "external", true},
		{"hold:po", "po", true},
		{"hold:review-board", "review-board", true},
		{"  Hold:External ", "External", true},
		{"HOLD:PO", "PO", true},
		{"hold: po ", "po", true},
		{"hold:\tpo\n", "po", true},
		{"hold::", ":", true},
		{"hold:cleared", "", false},
		{"hold:Cleared", "", false},
		{" hold: CLEARED ", "", false},
		{"hold:", "", false},
		{"hold:   ", "", false},
		{"hold", "", false},
		{"hold po", "", false},
		{"on-hold", "", false},
		{"mpr-human-hold", "", false},
		{"hold-po", "", false},
		{"xhold:po", "", false},
		{"", "", false},
	}
	for _, tc := range cases {
		gotValue, gotOK := ActiveHoldValue(tc.label)
		if gotValue != tc.wantValue || gotOK != tc.wantOK {
			t.Errorf("ActiveHoldValue(%q) = (%q, %v), want (%q, %v)", tc.label, gotValue, gotOK, tc.wantValue, tc.wantOK)
		}
	}
}

func TestHasActiveHold(t *testing.T) {
	cases := []struct {
		name   string
		labels []string
		want   bool
	}{
		{"nil", nil, false},
		{"unrelated", []string{"mysql-cutover", "mpr-human-hold", "on-hold", HumanLabel}, false},
		{"value the runtime always honored", []string{"x", "hold:external"}, true},
		{"pack-defined value", []string{"hold:po"}, true},
		{"unrelated pack-defined value", []string{"needs-review", "hold:release-train"}, true},
		{"cleared alone", []string{"hold:cleared"}, false},
		{"cleared beside an active hold", []string{"hold:cleared", "hold:po"}, true},
		{"active hold beside a later cleared", []string{"hold:po", "hold:cleared"}, true},
		{"empty value", []string{"hold:", "hold: "}, false},
	}
	for _, tc := range cases {
		if got := HasActiveHold(tc.labels); got != tc.want {
			t.Errorf("%s: HasActiveHold(%q) = %v, want %v", tc.name, tc.labels, got, tc.want)
		}
	}
}

func TestHasHumanLabel(t *testing.T) {
	cases := []struct {
		name   string
		labels []string
		want   bool
	}{
		{"nil", nil, false},
		{"unrelated", []string{"needs-review", "mpr-human-hold", "human-hold"}, false},
		{"a hold is not human", []string{"hold:external", "hold:po"}, false},
		{"human", []string{"x", HumanLabel}, true},
		{"space trimmed", []string{"  human "}, true},
		{"case kept, as bd's --exclude-label keeps it", []string{"Human"}, false},
	}
	for _, tc := range cases {
		if got := HasHumanLabel(tc.labels); got != tc.want {
			t.Errorf("%s: HasHumanLabel(%q) = %v, want %v", tc.name, tc.labels, got, tc.want)
		}
	}
}

// TestHumanLabelIsNotAHold keeps the two exclusions apart: the human label
// parks a bead for a person on the route-scoped tiers only, and a hold parks
// it on every work-serving path, assigned or not.
func TestHumanLabelIsNotAHold(t *testing.T) {
	if HasActiveHold([]string{HumanLabel}) {
		t.Fatalf("HasActiveHold(%q) = true, want false", HumanLabel)
	}
}
