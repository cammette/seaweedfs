package migrate

import "testing"

func TestStampAndSkipCenter(t *testing.T) {
	if got := StampCenter("photo.jpg", "a"); got != "a::photo.jpg" {
		t.Fatalf("stamp: %s", got)
	}
	if got := StampCenter("b::photo.jpg", "a"); got != "b::photo.jpg" {
		t.Fatalf("keep origin: %s", got)
	}
	if !SkipForeignCenter("b::photo.jpg", "b") {
		t.Fatal("same target center should skip")
	}
	if SkipForeignCenter("a::photo.jpg", "b") {
		t.Fatal("other center should copy")
	}
	if SkipForeignCenter("photo.jpg", "b") {
		t.Fatal("untagged name should copy")
	}
	if _, ok := FileCenter("::x"); ok {
		t.Fatal("empty center")
	}
}
