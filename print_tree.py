from pathlib import Path

def print_tree(dir_path: Path, prefix: str = '', ignore_dirs: set = None):
    # Các thư mục muốn bỏ qua không hiển thị (bạn có thể thêm bớt tùy ý)
    if ignore_dirs is None:
        ignore_dirs = {'.git', '.venv', 'venv', 'env', '__pycache__', 'node_modules', '.idea', '.vscode'}

    # Lấy danh sách tất cả các file/thư mục và sắp xếp theo tên
    # Lọc bỏ các thư mục nằm trong danh sách ignore_dirs
    paths = sorted([p for p in dir_path.iterdir() if p.name not in ignore_dirs])
    
    for count, path in enumerate(paths):
        is_last = (count == len(paths) - 1)
        # Nếu là thành phần cuối cùng thì dùng '└──', ngược lại dùng '├──'
        connector = '└── ' if is_last else '├── '
        
        # In file hoặc thư mục hiện tại
        print(f"{prefix}{connector}{path.name}")
        
        # Nếu là thư mục, bạn đi tiếp vào bên trong (đệ quy)
        if path.is_dir():
            extension = '    ' if is_last else '│   '
            print_tree(path, prefix=prefix + extension, ignore_dirs=ignore_dirs)

if __name__ == '__main__':
    # In thư mục hiện tại '.'
    current_directory = Path('.')
    print(f"{current_directory.resolve().name}/")
    print_tree(current_directory)
