package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseDoctorSection(t *testing.T) {
	data := []byte(`
[workspace]
name = "test-city"

[doctor]
worktree_rig_warn_size = "5GB"
worktree_rig_error_size = "30GB"
nested_worktree_prune = true

[[agent]]
name = "mayor"
`)
	cfg, err := Parse(data)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Doctor.WorktreeRigWarnSize != "5GB" {
		t.Errorf("WorktreeRigWarnSize = %q, want %q", cfg.Doctor.WorktreeRigWarnSize, "5GB")
	}
	if cfg.Doctor.WorktreeRigErrorSize != "30GB" {
		t.Errorf("WorktreeRigErrorSize = %q, want %q", cfg.Doctor.WorktreeRigErrorSize, "30GB")
	}
	if !cfg.Doctor.NestedWorktreePrune {
		t.Error("NestedWorktreePrune = false, want true")
	}
}

func TestParseNoDoctorSection(t *testing.T) {
	data := []byte(`
[workspace]
name = "test-city"

[[agent]]
name = "mayor"
`)
	cfg, err := Parse(data)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Doctor.WorktreeRigWarnSize != "" || cfg.Doctor.WorktreeRigErrorSize != "" {
		t.Errorf("Doctor section should be zero-valued; got %+v", cfg.Doctor)
	}
	if cfg.Doctor.NestedWorktreePrune {
		t.Error("NestedWorktreePrune defaults to true; want false")
	}

	// Unset Doctor must still return real defaults via accessor methods.
	if got := cfg.Doctor.WorktreeRigWarnBytes(); got != defaultWorktreeRigWarnBytes {
		t.Errorf("WorktreeRigWarnBytes() = %d, want %d", got, defaultWorktreeRigWarnBytes)
	}
	if got := cfg.Doctor.WorktreeRigErrorBytes(); got != defaultWorktreeRigErrorBytes {
		t.Errorf("WorktreeRigErrorBytes() = %d, want %d", got, defaultWorktreeRigErrorBytes)
	}
}

func TestMarshalOmitsEmptyDoctorSection(t *testing.T) {
	c := DefaultCity("test")
	data, err := c.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(data), "[doctor]") {
		t.Errorf("Marshal output should not contain '[doctor]' when empty:\n%s", data)
	}
}

func TestDoctorConfigByteAccessors(t *testing.T) {
	tests := []struct {
		name      string
		cfg       DoctorConfig
		wantWarn  int64
		wantError int64
	}{
		{
			name:      "empty falls back to defaults",
			cfg:       DoctorConfig{},
			wantWarn:  defaultWorktreeRigWarnBytes,
			wantError: defaultWorktreeRigErrorBytes,
		},
		{
			name:      "explicit GB values",
			cfg:       DoctorConfig{WorktreeRigWarnSize: "5GB", WorktreeRigErrorSize: "20GB"},
			wantWarn:  5 * 1024 * 1024 * 1024,
			wantError: 20 * 1024 * 1024 * 1024,
		},
		{
			name:      "MB and KB units",
			cfg:       DoctorConfig{WorktreeRigWarnSize: "500MB", WorktreeRigErrorSize: "2048MB"},
			wantWarn:  500 * 1024 * 1024,
			wantError: 2048 * 1024 * 1024,
		},
		{
			name:      "unparseable warn falls back to default; error still parses",
			cfg:       DoctorConfig{WorktreeRigWarnSize: "junk", WorktreeRigErrorSize: "100GB"},
			wantWarn:  defaultWorktreeRigWarnBytes,
			wantError: 100 * 1024 * 1024 * 1024,
		},
		{
			name:      "error < warn is clamped up to warn (monotonic)",
			cfg:       DoctorConfig{WorktreeRigWarnSize: "10GB", WorktreeRigErrorSize: "1GB"},
			wantWarn:  10 * 1024 * 1024 * 1024,
			wantError: 10 * 1024 * 1024 * 1024,
		},
		{
			name:      "negative or zero bytes treated as unset",
			cfg:       DoctorConfig{WorktreeRigWarnSize: "0GB", WorktreeRigErrorSize: "0"},
			wantWarn:  defaultWorktreeRigWarnBytes,
			wantError: defaultWorktreeRigErrorBytes,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.WorktreeRigWarnBytes(); got != tt.wantWarn {
				t.Errorf("WorktreeRigWarnBytes() = %d, want %d", got, tt.wantWarn)
			}
			if got := tt.cfg.WorktreeRigErrorBytes(); got != tt.wantError {
				t.Errorf("WorktreeRigErrorBytes() = %d, want %d", got, tt.wantError)
			}
		})
	}
}

func TestDoctorConfigPackScriptTimeout(t *testing.T) {
	tests := []struct {
		name string
		cfg  DoctorConfig
		want time.Duration
	}{
		{"empty falls back to default", DoctorConfig{}, defaultPackScriptTimeout},
		{"explicit positive seconds", DoctorConfig{PackScriptTimeoutSecs: 5}, 5 * time.Second},
		{"zero falls back to default", DoctorConfig{PackScriptTimeoutSecs: 0}, defaultPackScriptTimeout},
		{"negative falls back to default", DoctorConfig{PackScriptTimeoutSecs: -10}, defaultPackScriptTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.cfg.PackScriptTimeout(); got != tt.want {
				t.Errorf("PackScriptTimeout() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParsePackScriptTimeoutSection(t *testing.T) {
	data := []byte(`
[workspace]
name = "test-city"

[doctor]
pack_script_timeout_secs = 45

[[agent]]
name = "mayor"
`)
	cfg, err := Parse(data)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Doctor.PackScriptTimeoutSecs != 45 {
		t.Errorf("PackScriptTimeoutSecs = %d, want 45", cfg.Doctor.PackScriptTimeoutSecs)
	}
	if got := cfg.Doctor.PackScriptTimeout(); got != 45*time.Second {
		t.Errorf("PackScriptTimeout() = %v, want 45s", got)
	}
}

func TestParseHumanSize(t *testing.T) {
	tests := []struct {
		input  string
		want   int64
		wantOK bool
	}{
		{"", 0, false},
		{"   ", 0, false},
		{"junk", 0, false},
		{"10", 10, true},      // bytes implied
		{"1024B", 1024, true}, // explicit B suffix
		{"1KB", 1024, true},
		{"5 mb", 5 * 1024 * 1024, true}, // case-insensitive, whitespace tolerant
		{"  10gb ", 10 * 1024 * 1024 * 1024, true},
		{"-5GB", -5 * 1024 * 1024 * 1024, true}, // accessor treats negative as unset; parser is permissive
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, ok := parseHumanSize(tt.input)
			if ok != tt.wantOK {
				t.Errorf("ok = %v, want %v (input %q)", ok, tt.wantOK, tt.input)
			}
			if got != tt.want {
				t.Errorf("value = %d, want %d (input %q)", got, tt.want, tt.input)
			}
		})
	}
}
