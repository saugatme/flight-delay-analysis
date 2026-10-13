from flight_delay_analysis.dashboard import create_dashboard_app


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)


class FakeCollection:
    def __init__(self, rows):
        self.rows = rows

    def distinct(self, field):
        return list({row[field] for row in self.rows if field in row})

    def find(self, query, projection):
        return FakeCursor(self.rows)


class FakeDatabase:
    def __init__(self):
        self.collections = {
            "month_summary": FakeCollection(
                [{"report_year": 2025, "report_month": 1}]
            ),
            "season_summary": FakeCollection([{"season": "winter"}]),
            "carrier_summary": FakeCollection([{"reporting_airline": "AA"}]),
            "airport_summary": FakeCollection([{"airport": "AAA"}]),
            "route_summary": FakeCollection(
                [{"origin": "AAA", "destination": "BBB"}]
            ),
        }

    def __getitem__(self, name):
        return self.collections[name]


def _component_ids(component):
    found = set()
    identifier = getattr(component, "id", None)
    if identifier:
        found.add(identifier)
    children = getattr(component, "children", None)
    if children is None:
        return found
    if not isinstance(children, list):
        children = [children]
    for child in children:
        if hasattr(child, "children") or hasattr(child, "id"):
            found.update(_component_ids(child))
    return found


def test_dashboard_has_required_filters_and_views() -> None:
    app = create_dashboard_app(FakeDatabase())

    assert {
        "year",
        "month",
        "season",
        "carrier",
        "airport",
        "route",
        "carrier-chart",
        "trend-chart",
        "cancellation-chart",
        "score-chart",
        "network-map",
    } <= _component_ids(app.layout)
    assert len(app.callback_map) == 1
