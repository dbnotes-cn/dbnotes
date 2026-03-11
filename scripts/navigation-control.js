(function() {
    'use strict';

    // Get current page path
    function getCurrentPagePath() {
        var path = window.location.pathname;
        // Remove leading slash and .html extension
        return path.replace(/^\//, '').replace(/\.html$/, '');
    }

    // Get level of a path
    function getPathLevel(path) {
        if (!path || path === '.' || path === './') return 1;
        var parts = path.split('/').filter(function(part) {
            return part && part.length > 0;
        });
        return parts.length + 1;
    }

    // Control navigation buttons
    function controlNavigationButtons() {
        var currentPath = getCurrentPagePath();
        var currentLevel = getPathLevel(currentPath);
        
        // Get all navigation buttons
        var prevButton = document.querySelector('.navigation-prev');
        var nextButton = document.querySelector('.navigation-next');
        
        if (!prevButton && !nextButton) return;
        
        var prevHref = prevButton ? prevButton.getAttribute('href') : '';
        var nextHref = nextButton ? nextButton.getAttribute('href') : '';
        
        var prevLevel = prevHref ? getPathLevel(prevHref) : 0;
        var nextLevel = nextHref ? getPathLevel(nextHref) : 0;
        
        // Hide navigation buttons if they point to different levels (difference > 1)
        if (prevButton && Math.abs(prevLevel - currentLevel) > 1) {
            prevButton.style.display = 'none';
        } else if (prevButton) {
            prevButton.style.display = 'flex';
        }
        
        if (nextButton && Math.abs(nextLevel - currentLevel) > 1) {
            nextButton.style.display = 'none';
        } else if (nextButton) {
            nextButton.style.display = 'flex';
        }
        
        console.log('Current path:', currentPath, 'Level:', currentLevel);
        console.log('Prev:', prevHref, 'Level:', prevLevel);
        console.log('Next:', nextHref, 'Level:', nextLevel);
    }

    // Run control function when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function() {
            controlNavigationButtons();
        });
    } else {
        controlNavigationButtons();
    }

    // Also run after page navigation
    window.addEventListener('popstate', function() {
        setTimeout(controlNavigationButtons, 100);
    });

})();