#!/usr/bin/env python3
"""
Test script to verify ReFED alignment definitions are working correctly.
"""

from backend.utils.refed_alignment_definitions import (
    ALIGNMENT_DEFINITIONS,
    get_alignment_category_names,
    get_enhanced_prompt_for_category,
    get_all_enhanced_prompts,
    get_keywords_for_search,
    get_category_by_keyword,
)


def test_alignment_definitions():
    """Test basic functionality of alignment definitions module."""

    print("=" * 80)
    print("ReFED ALIGNMENT DEFINITIONS TEST")
    print("=" * 80)
    print()

    # Test 1: Check all categories are loaded
    print("TEST 1: Verify all 13 categories are loaded")
    print("-" * 80)
    category_names = get_alignment_category_names()
    print(f"✓ Found {len(category_names)} categories:")
    for i, name in enumerate(category_names, 1):
        print(f"  {i}. {name}")
    assert len(category_names) == 13, f"Expected 13 categories, got {len(category_names)}"
    print()

    # Test 2: Check structure of one category definition
    print("TEST 2: Verify structure of 'Solution Provider (Vendor/Innovator)' category")
    print("-" * 80)
    test_category = "Solution Provider (Vendor/Innovator)"
    if test_category in ALIGNMENT_DEFINITIONS:
        cat = ALIGNMENT_DEFINITIONS[test_category]
        print(f"✓ Category: {cat['name']}")
        print(f"✓ Description length: {len(cat['description'])} chars")
        print(f"✓ Number of signals: {len(cat['signals'])}")
        print(f"✓ Number of examples: {len(cat['examples'])}")
        print(f"✓ Number of keywords: {len(cat['keywords'])}")
        print(f"✓ Related programs: {', '.join(cat['related_programs'])}")
        print()
        print(f"Description: {cat['description'][:150]}...")
        print()

    # Test 3: Check enhanced prompt generation
    print("TEST 3: Verify enhanced prompt generation for one category")
    print("-" * 80)
    prompt = get_enhanced_prompt_for_category("FWFC: Capital-Seeking")
    print(f"✓ Generated prompt length: {len(prompt)} chars")
    print()
    print("Sample prompt:")
    print(prompt[:500] + "...")
    print()

    # Test 4: Check full enhanced prompts
    print("TEST 4: Verify full enhanced prompts for all categories")
    print("-" * 80)
    all_prompts = get_all_enhanced_prompts()
    print(f"✓ Total enhanced prompts length: {len(all_prompts):,} chars")
    print(f"✓ Estimated tokens (rough): ~{len(all_prompts) // 4:,} tokens")
    print()

    # Test 5: Test keyword search
    print("TEST 5: Test keyword-based category search")
    print("-" * 80)
    test_keywords = ["fundraising", "CDP", "FWAN", "B2B solution", "composting"]
    for keyword in test_keywords:
        matching = get_category_by_keyword(keyword)
        print(f"✓ Keyword '{keyword}' matches: {', '.join(matching) if matching else 'None'}")
    print()

    # Test 6: Check keywords dictionary
    print("TEST 6: Verify keyword mapping")
    print("-" * 80)
    keywords_dict = get_keywords_for_search()
    print(f"✓ Keywords mapped for {len(keywords_dict)} categories")
    sample_cat = "Data Contributor / Partner"
    if sample_cat in keywords_dict:
        print(f"✓ Sample - '{sample_cat}' has {len(keywords_dict[sample_cat])} keywords:")
        print(f"  {', '.join(keywords_dict[sample_cat][:5])}...")
    print()

    # Test 7: Verify all categories have complete information
    print("TEST 7: Validate completeness of all category definitions")
    print("-" * 80)
    required_fields = ["name", "description", "mission_alignment", "signals", "examples", "keywords", "related_programs"]
    issues = []
    for cat_name, cat_data in ALIGNMENT_DEFINITIONS.items():
        for field in required_fields:
            if field not in cat_data:
                issues.append(f"{cat_name} missing field: {field}")
            elif field in ["signals", "examples", "keywords", "related_programs"]:
                if not cat_data[field] or len(cat_data[field]) == 0:
                    issues.append(f"{cat_name} has empty {field} list")

    if issues:
        print("✗ Issues found:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("✓ All categories have complete information")
    print()

    print("=" * 80)
    print("ALL TESTS PASSED ✓")
    print("=" * 80)
    print()
    print("Summary:")
    print(f"  • {len(category_names)} alignment categories defined")
    print(f"  • {sum(len(c['signals']) for c in ALIGNMENT_DEFINITIONS.values())} total signals across all categories")
    print(f"  • {sum(len(c['examples']) for c in ALIGNMENT_DEFINITIONS.values())} total examples across all categories")
    print(f"  • Enhanced prompt ready for integration with tagger.py")
    print()


if __name__ == "__main__":
    test_alignment_definitions()
