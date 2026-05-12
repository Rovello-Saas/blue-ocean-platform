from src.research import aliexpress


def test_product_identity_keys_extracts_item_id_from_url():
    keys = aliexpress.product_identity_keys(
        {"title": "Portable LED Desk Lamp"},
        url="https://www.aliexpress.com/item/1005001234567890.html?src=foo",
    )

    assert "id:1005001234567890" in keys
    assert "title:portable led desk lamp" in keys


def test_search_products_skips_excluded_supplier(monkeypatch):
    monkeypatch.setattr(aliexpress, "ALIEXPRESS_APP_KEY", "test-key")

    products = [
        {
            "aliexpress_product_id": "111",
            "title": "Laser Lampe fuer Zuhause",
            "url": "https://www.aliexpress.com/item/111.html",
            "price": 10,
            "rating": 4.8,
            "orders": 5000,
        },
        {
            "aliexpress_product_id": "222",
            "title": "Laser Lampe fuer Zuhause",
            "url": "https://www.aliexpress.com/item/222.html",
            "price": 12,
            "rating": 4.7,
            "orders": 3000,
        },
    ]

    def fake_browse_feed(**kwargs):
        return products if kwargs.get("page_no") == 1 else []

    monkeypatch.setattr(aliexpress, "browse_feed", fake_browse_feed)

    matches = aliexpress.search_products(
        keyword="laser lampe",
        language="de",
        min_rating=0,
        min_orders=0,
        max_results=1,
        exclude_product_keys={"id:111"},
    )

    assert [p["aliexpress_product_id"] for p in matches] == ["222"]
