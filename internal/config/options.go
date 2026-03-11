package config

import "fmt"

// ResolveOptions validates user-specified options against a provider's schema
// and produces extra CLI args to inject into the command. Options not specified
// by the user have their schema defaults applied. Returns the extra args and
// metadata entries (opt_<key>=<value>) for bead persistence.
//
// Args are emitted in schema declaration order for deterministic command lines.
func ResolveOptions(schema []ProviderOption, options map[string]string) (extraArgs []string, metadata map[string]string, err error) {
	metadata = make(map[string]string)

	// Validate user-specified option keys and values up front.
	for key, value := range options {
		opt := findOption(schema, key)
		if opt == nil {
			return nil, nil, fmt.Errorf("%w: %s", ErrUnknownOption, key)
		}
		if findChoice(opt.Choices, value) == nil {
			return nil, nil, fmt.Errorf("invalid value for %s: %s", key, value)
		}
	}

	// Iterate in schema declaration order for deterministic arg ordering.
	for _, opt := range schema {
		if value, ok := options[opt.Key]; ok {
			choice := findChoice(opt.Choices, value)
			extraArgs = append(extraArgs, choice.FlagArgs...)
			metadata["opt_"+opt.Key] = value
		} else if opt.Default != "" {
			choice := findChoice(opt.Choices, opt.Default)
			if choice != nil {
				extraArgs = append(extraArgs, choice.FlagArgs...)
			}
			// Defaults are NOT written to metadata — only explicit choices are persisted.
		}
	}

	return extraArgs, metadata, nil
}

func findOption(schema []ProviderOption, key string) *ProviderOption {
	for i := range schema {
		if schema[i].Key == key {
			return &schema[i]
		}
	}
	return nil
}

func findChoice(choices []OptionChoice, value string) *OptionChoice {
	for i := range choices {
		if choices[i].Value == value {
			return &choices[i]
		}
	}
	return nil
}
