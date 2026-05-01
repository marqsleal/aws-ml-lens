from pathlib import Path

from setuptools import find_packages, setup

BASE_DIR = Path(__file__).resolve().parent


def parse_requirements(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]


setup(
    name="aws-ml-lens",
    version="0.1.0",
    description="Pipeline and tooling for AWS ML Lens project",
    author="aws-ml-lens",
    python_requires=">=3.10",
    packages=find_packages(include=["src", "src.*"]),
    install_requires=parse_requirements(BASE_DIR / "requirements.txt"),
    include_package_data=True,
    zip_safe=False,
)
