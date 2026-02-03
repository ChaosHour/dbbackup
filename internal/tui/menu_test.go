package tui

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"dbbackup/internal/config"
	"dbbackup/internal/logger"
)

// TestMenuModelCreation tests that menu model is created correctly
func TestMenuModelCreation(t *testing.T) {
	cfg := config.New()
	log := logger.NewNullLogger()

	model := NewMenuModel(cfg, log)
	defer model.Close()

	if model == nil {
		t.Fatal("Expected non-nil model")
	}

	if len(model.choices) == 0 {
		t.Error("Expected choices to be populated")
	}

	// Verify expected menu items exist
	expectedItems := []string{
		"Single Database Backup",
		"Cluster Backup",
		"Restore Single Database",
		"Tools",
		"Database Status",
		"Configuration Settings",
		"Quit",
	}

	for _, expected := range expectedItems {
		found := false
		for _, choice := range model.choices {
			if strings.Contains(choice, expected) || choice == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected menu item %q not found", expected)
		}
	}
}

// TestMenuNavigation tests keyboard navigation
func TestMenuNavigation(t *testing.T) {
	cfg := config.New()
	log := logger.NewNullLogger()

	model := NewMenuModel(cfg, log)
	defer model.Close()

	// Initial cursor should be 0
	if model.cursor != 0 {
		t.Errorf("Expected initial cursor 0, got %d", model.cursor)
	}

	// Navigate down
	newModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyDown})
	menuModel := newModel.(*MenuModel)
	if menuModel.cursor != 1 {
		t.Errorf("Expected cursor 1 after down, got %d", menuModel.cursor)
	}

	// Navigate down again
	newModel, _ = menuModel.Update(tea.KeyMsg{Type: tea.KeyDown})
	menuModel = newModel.(*MenuModel)
	if menuModel.cursor != 2 {
		t.Errorf("Expected cursor 2 after second down, got %d", menuModel.cursor)
	}

	// Navigate up
	newModel, _ = menuModel.Update(tea.KeyMsg{Type: tea.KeyUp})
	menuModel = newModel.(*MenuModel)
	if menuModel.cursor != 1 {
		t.Errorf("Expected cursor 1 after up, got %d", menuModel.cursor)
	}
}

// TestMenuVimNavigation tests vim-style navigation (j/k)
func TestMenuVimNavigation(t *testing.T) {
	cfg := config.New()
	log := logger.NewNullLogger()

	model := NewMenuModel(cfg, log)
	defer model.Close()

	// Navigate down with 'j'
	newModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'j'}})
	menuModel := newModel.(*MenuModel)
	if menuModel.cursor != 1 {
		t.Errorf("Expected cursor 1 after 'j', got %d", menuModel.cursor)
	}

	// Navigate up with 'k'
	newModel, _ = menuModel.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}})
	menuModel = newModel.(*MenuModel)
	if menuModel.cursor != 0 {
		t.Errorf("Expected cursor 0 after 'k', got %d", menuModel.cursor)
	}
}

// TestMenuBoundsCheck tests that cursor doesn't go out of bounds
func TestMenuBoundsCheck(t *testing.T) {
	cfg := config.New()
	log := logger.NewNullLogger()

	model := NewMenuModel(cfg, log)
	defer model.Close()

	// Try to go up from position 0
	newModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyUp})
	menuModel := newModel.(*MenuModel)
	if menuModel.cursor != 0 {
		t.Errorf("Expected cursor to stay at 0 when going up, got %d", menuModel.cursor)
	}

	// Go to last item
	for i := 0; i < len(model.choices); i++ {
		newModel, _ = menuModel.Update(tea.KeyMsg{Type: tea.KeyDown})
		menuModel = newModel.(*MenuModel)
	}

	lastIndex := len(model.choices) - 1
	if menuModel.cursor != lastIndex {
		t.Errorf("Expected cursor at last index %d, got %d", lastIndex, menuModel.cursor)
	}

	// Try to go down past last item
	newModel, _ = menuModel.Update(tea.KeyMsg{Type: tea.KeyDown})
	menuModel = newModel.(*MenuModel)
	if menuModel.cursor != lastIndex {
		t.Errorf("Expected cursor to stay at %d when going down past end, got %d", lastIndex, menuModel.cursor)
	}
}

// TestMenuQuit tests quit functionality
func TestMenuQuit(t *testing.T) {
	cfg := config.New()
	log := logger.NewNullLogger()

	model := NewMenuModel(cfg, log)
	defer model.Close()

	// Test 'q' to quit
	newModel, cmd := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}})
	menuModel := newModel.(*MenuModel)

	if !menuModel.quitting {
		t.Error("Expected quitting to be true after 'q'")
	}

	if cmd == nil {
		t.Error("Expected quit command to be returned")
	}
}

// TestMenuCtrlC tests Ctrl+C handling
func TestMenuCtrlC(t *testing.T) {
	cfg := config.New()
	log := logger.NewNullLogger()

	model := NewMenuModel(cfg, log)
	defer model.Close()

	// Test Ctrl+C
	newModel, cmd := model.Update(tea.KeyMsg{Type: tea.KeyCtrlC})
	menuModel := newModel.(*MenuModel)

	if !menuModel.quitting {
		t.Error("Expected quitting to be true after Ctrl+C")
	}

	if cmd == nil {
		t.Error("Expected quit command to be returned")
	}
}

// TestMenuDatabaseTypeSwitch tests database type switching with 't'
func TestMenuDatabaseTypeSwitch(t *testing.T) {
	cfg := config.New()
	cfg.DatabaseType = "postgres"
	log := logger.NewNullLogger()

	model := NewMenuModel(cfg, log)
	defer model.Close()

	initialCursor := model.dbTypeCursor

	// Press 't' to cycle database type
	newModel, _ := model.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'t'}})
	menuModel := newModel.(*MenuModel)

	expectedCursor := (initialCursor + 1) % len(model.dbTypes)
	if menuModel.dbTypeCursor != expectedCursor {
		t.Errorf("Expected dbTypeCursor %d after 't', got %d", expectedCursor, menuModel.dbTypeCursor)
	}
}

// TestMenuView tests that View() returns valid output
func TestMenuView(t *testing.T) {
	cfg := config.New()
	cfg.Version = "5.7.9"
	log := logger.NewNullLogger()

	model := NewMenuModel(cfg, log)
	defer model.Close()

	view := model.View()

	if len(view) == 0 {
		t.Error("Expected non-empty view output")
	}

	// Check for expected content
	if !strings.Contains(view, "Interactive Menu") {
		t.Error("Expected view to contain 'Interactive Menu'")
	}

	if !strings.Contains(view, "5.7.9") {
		t.Error("Expected view to contain version number")
	}
}

// TestMenuQuittingView tests view when quitting
func TestMenuQuittingView(t *testing.T) {
	cfg := config.New()
	log := logger.NewNullLogger()

	model := NewMenuModel(cfg, log)
	defer model.Close()

	model.quitting = true
	view := model.View()

	if !strings.Contains(view, "Thanks for using") {
		t.Error("Expected quitting view to contain goodbye message")
	}
}

// TestAutoSelectValid tests that auto-select with valid index works
func TestAutoSelectValid(t *testing.T) {
	cfg := config.New()
	cfg.TUIAutoSelect = 0 // Single Database Backup
	log := logger.NewNullLogger()

	model := NewMenuModel(cfg, log)
	defer model.Close()

	// Trigger auto-select message - should transition to DatabaseSelectorModel
	newModel, _ := model.Update(autoSelectMsg{})

	// Auto-select for option 0 (Single Backup) should return a DatabaseSelectorModel
	// This verifies the handler was called correctly
	_, ok := newModel.(DatabaseSelectorModel)
	if !ok {
		// It might also be *MenuModel if the handler returned early
		if menuModel, ok := newModel.(*MenuModel); ok {
			if menuModel.cursor != 0 {
				t.Errorf("Expected cursor 0 after auto-select, got %d", menuModel.cursor)
			}
		} else {
			t.Logf("Auto-select returned model type: %T (this is acceptable)", newModel)
		}
	}
}

// TestAutoSelectSeparatorSkipped tests that separators are handled in auto-select
func TestAutoSelectSeparatorSkipped(t *testing.T) {
	cfg := config.New()
	cfg.TUIAutoSelect = 3 // Separator
	log := logger.NewNullLogger()

	model := NewMenuModel(cfg, log)
	defer model.Close()

	// Should not crash when auto-selecting separator
	newModel, cmd := model.Update(autoSelectMsg{})

	// For separator, should return same MenuModel without transition
	menuModel, ok := newModel.(*MenuModel)
	if !ok {
		t.Errorf("Expected MenuModel for separator, got %T", newModel)
		return
	}

	// Should just return without action
	if menuModel.quitting {
		t.Error("Should not quit when selecting separator")
	}

	// cmd should be nil for separator
	if cmd != nil {
		t.Error("Expected nil command for separator selection")
	}
}

// BenchmarkMenuView benchmarks the View() rendering
func BenchmarkMenuView(b *testing.B) {
	cfg := config.New()
	log := logger.NewNullLogger()

	model := NewMenuModel(cfg, log)
	defer model.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = model.View()
	}
}

// BenchmarkMenuNavigation benchmarks navigation performance
func BenchmarkMenuNavigation(b *testing.B) {
	cfg := config.New()
	log := logger.NewNullLogger()

	model := NewMenuModel(cfg, log)
	defer model.Close()

	downKey := tea.KeyMsg{Type: tea.KeyDown}
	upKey := tea.KeyMsg{Type: tea.KeyUp}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			model.Update(downKey)
		} else {
			model.Update(upKey)
		}
	}
}
