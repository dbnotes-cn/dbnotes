(function() {
    console.log('Secure download script loaded');
    
    const fileMap = {
        'Autoinstall_DM8': atob('aHR0cHM6Ly93d3cuZGJub3Rlcy5jbi9kYm5vdGVzL2ZpbGUvQXV0b2luc3RhbGxfRE04LnRhci5neg=='),
        'Autoinstall_KESv9_new': atob('aHR0cHM6Ly93d3cuZGJub3Rlcy5jbi9kYm5vdGVzL2ZpbGUvQXV0b2luc3RhbGxfS0VTdjlfbmV3LnRhci5neg=='),
        'Autoinstall_KESv9_old': atob('aHR0cHM6Ly93d3cuZGJub3Rlcy5jbi9kYm5vdGVzL2ZpbGUvQXV0b2luc3RhbGxfS0VTdjlfb2xkLnRhci5neg=='),
        'unique_sql': atob('aHR0cHM6Ly93d3cuZGJub3Rlcy5jbi9kYm5vdGVzL2ZpbGUvdW5pcXVlX3NxbC5weQ=='),
        'unique_sql_plan': atob('aHR0cHM6Ly93d3cuZGJub3Rlcy5jbi9kYm5vdGVzL2ZpbGUvdW5pcXVlX3NxbF9wbGFuLnB5')
    };

    function handleDownload(fileId) {
        const actualUrl = fileMap[fileId];
        if (actualUrl) {
            const link = document.createElement('a');
            link.href = actualUrl;
            link.download = '';
            link.style.display = 'none';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            return true;
        }
        return false;
    }

    document.addEventListener('click', function(e) {
        const target = e.target;
        
        if (target.classList.contains('secure-download-link')) {
            const fileId = target.getAttribute('data-file-id');
            console.log('Link clicked, fileId:', fileId);
            e.preventDefault();
            e.stopPropagation();
            handleDownload(fileId);
        }
    }, true);
    
    console.log('Secure download initialization complete');
})();
